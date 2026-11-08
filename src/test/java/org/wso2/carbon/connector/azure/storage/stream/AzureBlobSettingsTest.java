/*
 * Copyright (c) 2026, WSO2 LLC. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.carbon.connector.azure.storage.stream;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** The parse that deployment-time validation and the runtime share. */
public class AzureBlobSettingsTest {

    private static final long MIB = 1024L * 1024L;
    private static final String OP = "azure.blobStreamWrite";

    private static Map<String, String> params(String... keyThenValue) {
        Map<String, String> values = new HashMap<>();
        values.put("container", "inbox");
        values.put("blob", "reports/out.jsonl");
        for (int i = 0; i < keyThenValue.length; i += 2) {
            values.put(keyThenValue[i], keyThenValue[i + 1]);
        }
        return values;
    }

    /**
     * One block per checkpoint unless the deployer says otherwise, and the deployer may say
     * anything at or above 1. Nothing validates the number against the block size or the source —
     * how many blocks a failure may re-upload is theirs to size (ADR-0025).
     */
    @Test
    public void theCadenceDefaultsToOneBlockAndIsTheDeployersOtherwise() {
        assertEquals(1, AzureBlobSettings.from(params(), OP).checkpointFrequency());
        assertEquals(64, AzureBlobSettings.from(params("checkpointFrequency", "64"), OP)
                .checkpointFrequency());
    }

    /** Decidable without an account, so it fails at deployment rather than mid-upload. */
    @Test
    public void anUnusableCadenceIsRefusedAtParseTime() {
        for (String bad : new String[]{"often", "0", "-4"}) {
            try {
                AzureBlobSettings.from(params("checkpointFrequency", bad), OP);
                fail("expected rejection of checkpointFrequency '" + bad + "'");
            } catch (IllegalArgumentException expected) {
                assertTrue(expected.getMessage(),
                        expected.getMessage().contains("checkpointFrequency"));
            }
        }
    }

    /**
     * Every parameter the parser reads must be one an operator actually looks up.
     *
     * <p>This is the test the {@code blockSizeUnit} bug needed. The operators do not hand
     * {@code from} the deployer's whole configuration — they walk a list of names, call
     * {@code lookupTemplateParamater} for each, and build a map from what came back. A parameter
     * missing from that list is never looked up, so it reaches {@code from} as absent and silently
     * takes its default, however correctly the template declares it and however well the parser
     * handles it.
     *
     * <p>So the configuration here is deliberately filtered through {@code PARAMETERS} first, the
     * way an operator does, rather than passed straight in. Passing it straight in is what every
     * other test in this class does, and it is precisely why they all passed while a real transfer
     * failed with "blockSize ... was 4 bytes".
     */
    @Test
    public void everyParameterSurvivesTheOperatorsLookup() {
        Map<String, String> written = new HashMap<>();
        written.put("container", "inbox");
        written.put("blob", "reports/out.jsonl");
        written.put("blockSize", "32");
        written.put("blockSizeUnit", "MB");
        written.put("contentType", "application/json");
        written.put("overwrite", "false");
        written.put("uploadConcurrency", "8");
        written.put("verifyBlocks", "false");
        written.put("readBlockSize", "16");
        written.put("readBlockSizeUnit", "MB");
        written.put("readConcurrency", "2");
        written.put("readTimeout", "120");
        written.put("checkpointFrequency", "64");

        // Exactly what AzureBlobStreamWrite.settings() and AzureBlobStreamRead do.
        Map<String, String> looked = new HashMap<>();
        for (String parameter : AzureBlobSettings.PARAMETERS) {
            String value = written.get(parameter);
            if (value != null) {
                looked.put(parameter, value);
            }
        }
        assertEquals("a parameter the parser reads is missing from PARAMETERS, so no operator will"
                + " ever look it up: " + difference(written, looked), written.size(), looked.size());

        AzureBlobSettings s = AzureBlobSettings.from(looked, OP);
        assertEquals("blockSizeUnit must apply", 32 * MIB, s.configuredBlockSize().longValue());
        assertEquals("readBlockSizeUnit must apply", 16 * MIB, s.readBlockSize());
        assertEquals("checkpointFrequency must arrive", 64, s.checkpointFrequency());
        assertEquals(8, s.uploadConcurrency());
        assertEquals(2, s.readConcurrency());
        assertEquals("application/json", s.contentType());
    }

    private static String difference(Map<String, String> written, Map<String, String> looked) {
        return written.keySet().stream().filter(k -> !looked.containsKey(k)).sorted().toList()
                .toString();
    }

    @Test
    public void readsTheMandatoryPair() {
        AzureBlobSettings s = AzureBlobSettings.from(params(), OP);
        assertEquals("inbox", s.container());
        assertEquals("reports/out.jsonl", s.blob());
    }

    /** An unset template parameter arrives as an empty string, not as null. */
    @Test
    public void blankIsTreatedAsAbsent() {
        Map<String, String> values = params("contentType", "   ", "blockSize", "");
        AzureBlobSettings s = AzureBlobSettings.from(values, OP);
        assertNull(s.contentType());
        assertNull("blank blockSize must mean 'derive it'", s.configuredBlockSize());

        for (String missing : new String[]{"container", "blob"}) {
            Map<String, String> broken = params(missing, "  ");
            try {
                AzureBlobSettings.from(broken, OP);
                fail("expected [" + missing + "] to be required");
            } catch (IllegalArgumentException expected) {
                assertTrue(expected.getMessage().contains(missing));
            }
        }
    }

    /**
     * The unit is its own parameter, so a form can offer it as a dropdown. The binary spellings mean
     * the same size as the plain ones, and a bare number is bytes.
     */
    @Test
    public void aSizeIsANumberPlusItsOwnUnitParameter() {
        for (String unit : new String[]{"MB", "MiB", "mib"}) {
            assertEquals(unit, 32 * MIB, AzureBlobSettings
                    .from(params("blockSize", "32", "blockSizeUnit", unit), OP)
                    .configuredBlockSize().longValue());
        }
        assertEquals("a bare number is bytes", 32 * MIB, AzureBlobSettings
                .from(params("blockSize", "33554432"), OP).configuredBlockSize().longValue());
        // 4096 KB is 4 MiB: it still has to land inside Azure's 1 MiB..4000 MiB range, which
        // from(...) checks at parse time so a bad value is caught at deployment.
        assertEquals(4 * MIB, AzureBlobSettings.from(params("blockSize", "4096", "blockSizeUnit", "KB"), OP)
                .configuredBlockSize().longValue());
        assertEquals(2L * 1024 * MIB, AzureBlobSettings.from(params("blockSize", "2", "blockSizeUnit", "GB"), OP)
                .configuredBlockSize().longValue());
    }

    /**
     * A unit written into the number is caught with a message that says where it belongs. This is
     * exactly what someone who has not seen the unit field will type, so the fix has to be in the
     * message rather than in a format reference.
     */
    @Test
    public void aUnitInsideTheNumberIsRefusedByName() {
        try {
            AzureBlobSettings.from(params("blockSize", "32MB"), OP);
            fail("expected a suffixed amount to be refused");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("blockSizeUnit"));
        }
    }

    @Test
    public void nonsenseSizesAreRefusedWithTheParameterNamed() {
        for (String bad : new String[]{"lots", "-4", "0", ""}) {
            Map<String, String> values = params("blockSize", bad);
            if (bad.isEmpty()) {
                assertNull(AzureBlobSettings.from(values, OP).configuredBlockSize());
                continue;
            }
            try {
                AzureBlobSettings.from(values, OP);
                fail("expected rejection of blockSize '" + bad + "'");
            } catch (IllegalArgumentException expected) {
                assertTrue(expected.getMessage(), expected.getMessage().contains("blockSize"));
            }
        }
    }

    /** Below Azure's minimum or above its 4000 MiB maximum, caught at deployment not at run time. */
    @Test
    public void blockSizesOutsideAzuresRangeAreRefusedAtParseTime() {
        for (String[] bad : new String[][]{{"512", "KB"}, {"8", "GB"}}) {
            try {
                AzureBlobSettings.from(params("blockSize", bad[0], "blockSizeUnit", bad[1]), OP);
                fail("expected rejection of " + bad[0] + bad[1]);
            } catch (IllegalArgumentException expected) {
                assertTrue(expected.getMessage(), expected.getMessage().contains("blockSize"));
            }
        }
    }

    @Test
    public void concurrencyDefaultsToFourAndIsBounded() {
        AzureBlobSettings s = AzureBlobSettings.from(params(), OP);
        assertEquals(AzureBlobSettings.DEFAULT_CONCURRENCY, s.uploadConcurrency());
        assertEquals(AzureBlobSettings.DEFAULT_CONCURRENCY, s.readConcurrency());

        assertEquals(1, AzureBlobSettings.from(params("uploadConcurrency", "1"), OP).uploadConcurrency());

        // Not capped at 16 any more. The bound is a typo guard at 256; how much heap the buffers
        // take is the deployer's call, recorded in the log rather than refused.
        assertEquals(64, AzureBlobSettings.from(params("uploadConcurrency", "64"), OP).uploadConcurrency());

        for (String bad : new String[]{"0", "257", "-1", "four"}) {
            try {
                AzureBlobSettings.from(params("uploadConcurrency", bad), OP);
                fail("expected rejection of uploadConcurrency " + bad);
            } catch (IllegalArgumentException expected) {
                assertTrue(expected.getMessage().contains("uploadConcurrency"));
            }
        }
    }

    @Test
    public void overwriteDefaultsToTrueAndRejectsNonBooleans() {
        assertTrue(AzureBlobSettings.from(params(), OP).overwrite());
        assertTrue(!AzureBlobSettings.from(params("overwrite", "FALSE"), OP).overwrite());
        try {
            AzureBlobSettings.from(params("overwrite", "yes"), OP);
            fail("expected rejection of overwrite 'yes'");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("overwrite"));
        }
    }

    /** Integrity checking is on unless a deployment has measured the cost and wants it back. */
    @Test
    public void blockVerificationIsOnByDefaultAndCanBeSwitchedOff() {
        assertTrue(AzureBlobSettings.from(params(), OP).verifyBlocks());
        assertTrue(AzureBlobSettings.from(params("verifyBlocks", "true"), OP).verifyBlocks());
        assertTrue(!AzureBlobSettings.from(params("verifyBlocks", "false"), OP).verifyBlocks());
        assertTrue("blank means unset, so the default stands",
                AzureBlobSettings.from(params("verifyBlocks", "  "), OP).verifyBlocks());
        try {
            AzureBlobSettings.from(params("verifyBlocks", "sometimes"), OP);
            fail("expected rejection of verifyBlocks 'sometimes'");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("verifyBlocks"));
        }
    }

    /**
     * readBlockSize is the high-leverage download setting, and it trades against the range hash:
     * Azure only computes a Content-MD5 for ranges at or below 4 MiB.
     */
    @Test
    public void readBlockSizeTradesRequestCountAgainstRangeVerification() {
        AzureBlobSettings dflt = AzureBlobSettings.from(params(), OP);
        assertEquals(AzureBlobSettings.DEFAULT_READ_BLOCK, dflt.readBlockSize());

        assertTrue("4 MiB is hashable",
                AzureBlobSettings.from(params("readBlockSize", "4", "readBlockSizeUnit", "MB"), OP).verifyRanges());
        assertTrue("above 4 MiB Azure will not hash, so verification must report itself off",
                !AzureBlobSettings.from(params("readBlockSize", "8", "readBlockSizeUnit", "MB"), OP).verifyRanges());
        assertTrue("verifyBlocks=false switches it off regardless of size",
                !AzureBlobSettings.from(params("readBlockSize", "4", "readBlockSizeUnit", "MB", "verifyBlocks", "false"), OP)
                        .verifyRanges());

        for (String[] bad : new String[][]{{"512", "KB"}, {"128", "MB"}}) {
            try {
                AzureBlobSettings.from(params("readBlockSize", bad[0], "readBlockSizeUnit", bad[1]), OP);
                fail("expected rejection of readBlockSize " + bad[0] + bad[1]);
            } catch (IllegalArgumentException expected) {
                assertTrue(expected.getMessage().contains("readBlockSize"));
            }
        }
    }

    /**
     * The deployer decides how much heap to spend; the connector refuses only arithmetic that cannot
     * run. Same stance as checkpointFrequency, which is validated against nothing and merely recorded.
     */
    @Test
    public void largeBuffersAreAllowedAndOnlyTheImpossibleIsRefused() {
        long heap = Runtime.getRuntime().maxMemory();
        long quarter = (long) (heap * AzureBlobSettings.READ_BUFFER_HEAP_SHARE);

        // Above the remark-worthy share but below the heap: allowed, and flagged for the log.
        int aggressive = (int) (quarter / (8 * MIB)) + 1;
        if (aggressive <= AzureBlobSettings.MAX_CONCURRENCY
                && (long) aggressive * 8 * MIB < heap) {
            AzureBlobSettings big = AzureBlobSettings.from(
                    params("readBlockSize", "8", "readBlockSizeUnit", "MB", "readConcurrency", String.valueOf(aggressive)), OP);
            assertEquals(aggressive, big.readConcurrency());
            assertTrue("should be worth a log line", big.readBuffersWorthRemarkingOn());
        }

        // Modest settings are allowed and say nothing.
        assertTrue(!AzureBlobSettings.from(params("readBlockSize", "4", "readBlockSizeUnit", "MB", "readConcurrency", "2"), OP)
                .readBuffersWorthRemarkingOn());

        // At or above the whole heap there is nothing to debate.
        int impossible = (int) (heap / (64 * MIB)) + 1;
        if (impossible <= AzureBlobSettings.MAX_CONCURRENCY) {
            try {
                AzureBlobSettings.from(
                        params("readBlockSize", "64", "readBlockSizeUnit", "MB", "readConcurrency", String.valueOf(impossible)), OP);
                fail("expected refusal of buffers at or above the entire heap");
            } catch (IllegalArgumentException expected) {
                assertTrue(expected.getMessage(), expected.getMessage().contains("cannot run"));
            }
        }
    }

    /** Unconfigured, the size is derived from the probed source so that the blob actually fits. */
    @Test
    public void anUnconfiguredBlockSizeIsDerivedFromTheSource() {
        AzureBlobSettings s = AzureBlobSettings.from(params(), OP);
        assertEquals(BlockSizing.DEFAULT_FLOOR, s.blockSizeFor(0));
        long forFiveTib = s.blockSizeFor(5L * 1024 * 1024 * MIB);
        assertTrue("must grow to fit a 5 TiB source", forFiveTib > BlockSizing.DEFAULT_FLOOR);
    }

    /** A configured size that cannot hold the probed source is refused once the source is known. */
    @Test
    public void aConfiguredBlockSizeIsCheckedAgainstTheProbedSource() {
        AzureBlobSettings s = AzureBlobSettings.from(params("blockSize", "4", "blockSizeUnit", "MB"), OP);
        assertEquals(4 * MIB, s.blockSizeFor(100L * MIB));
        try {
            s.blockSizeFor(1024L * 1024 * MIB);
            fail("4 MiB cannot hold a 1 TiB source");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("blocks"));
        }
    }

    /**
     * The upload side gets the same guard, and its message has to name the upload parameters.
     *
     * <p>It matters more here than on the read side, because an upload block defaults to 32 MiB
     * against a read block's 8 MiB — so the same concurrency figure holds four times the heap, and a
     * deployer who moved uploadConcurrency up after reading about the read side gets there sooner.
     */
    @Test
    public void theBudgetGuardNamesWhicheverSideIsImpossible() {
        long heap = Runtime.getRuntime().maxMemory();
        int impossible = (int) (heap / (32 * MIB)) + 1;
        try {
            AzureBlobSettings.checkBufferBudget(32L * MIB, impossible,
                    "blockSize", "uploadConcurrency", "azure.blobStreamWrite");
            fail("expected refusal of upload buffers at or above the entire heap");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("uploadConcurrency"));
            assertTrue("must name the size parameter too, since either one can be lowered",
                    expected.getMessage().contains("blockSize"));
            assertFalse("the upload message must not talk about reads",
                    expected.getMessage().contains("readConcurrency"));
        }
    }

    /** A single buffer of any sane size is never refused, whichever side asks. */
    @Test
    public void oneBufferIsAlwaysAffordable() {
        AzureBlobSettings.checkBufferBudget(32L * MIB, 1, "blockSize", "uploadConcurrency", "op");
        AzureBlobSettings.checkBufferBudget(8L * MIB, 1, "readBlockSize", "readConcurrency", "op");
    }
}
