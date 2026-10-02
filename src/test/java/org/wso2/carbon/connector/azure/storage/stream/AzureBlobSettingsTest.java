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

    @Test
    public void sizeSuffixesAreBinaryAndInterchangeable() {
        for (String written : new String[]{"33554432", "32M", "32MB", "32MiB", "32mib"}) {
            assertEquals(written, 32 * MIB,
                    AzureBlobSettings.from(params("blockSize", written), OP).configuredBlockSize().longValue());
        }
        // 4096K is 4 MiB: a K suffix still has to land inside Azure's 1 MiB..4000 MiB range,
        // which from(...) checks at parse time so a bad value is caught at deployment.
        assertEquals(4 * MIB, AzureBlobSettings.from(params("blockSize", "4096K"), OP)
                .configuredBlockSize().longValue());
        assertEquals(2 * 1024 * MIB, AzureBlobSettings.from(params("blockSize", "2G"), OP)
                .configuredBlockSize().longValue());
    }

    @Test
    public void nonsenseSizesAreRefusedWithTheParameterNamed() {
        for (String bad : new String[]{"lots", "-4M", "0", "4Q", ""}) {
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
        for (String bad : new String[]{"512K", "8G"}) {
            try {
                AzureBlobSettings.from(params("blockSize", bad), OP);
                fail("expected rejection of " + bad);
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
                AzureBlobSettings.from(params("readBlockSize", "4M"), OP).verifyRanges());
        assertTrue("above 4 MiB Azure will not hash, so verification must report itself off",
                !AzureBlobSettings.from(params("readBlockSize", "8M"), OP).verifyRanges());
        assertTrue("verifyBlocks=false switches it off regardless of size",
                !AzureBlobSettings.from(params("readBlockSize", "4M", "verifyBlocks", "false"), OP)
                        .verifyRanges());

        for (String bad : new String[]{"512K", "128M"}) {
            try {
                AzureBlobSettings.from(params("readBlockSize", bad), OP);
                fail("expected rejection of readBlockSize " + bad);
            } catch (IllegalArgumentException expected) {
                assertTrue(expected.getMessage().contains("readBlockSize"));
            }
        }
    }

    /**
     * The deployer decides how much heap to spend; the connector refuses only arithmetic that cannot
     * run. Same stance as maxReprocessed, which is validated against nothing and merely recorded.
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
                    params("readBlockSize", "8M", "readConcurrency", String.valueOf(aggressive)), OP);
            assertEquals(aggressive, big.readConcurrency());
            assertTrue("should be worth a log line", big.readBuffersWorthRemarkingOn());
        }

        // Modest settings are allowed and say nothing.
        assertTrue(!AzureBlobSettings.from(params("readBlockSize", "4M", "readConcurrency", "2"), OP)
                .readBuffersWorthRemarkingOn());

        // At or above the whole heap there is nothing to debate.
        int impossible = (int) (heap / (64 * MIB)) + 1;
        if (impossible <= AzureBlobSettings.MAX_CONCURRENCY) {
            try {
                AzureBlobSettings.from(
                        params("readBlockSize", "64M", "readConcurrency", String.valueOf(impossible)), OP);
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
        AzureBlobSettings s = AzureBlobSettings.from(params("blockSize", "4M"), OP);
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
