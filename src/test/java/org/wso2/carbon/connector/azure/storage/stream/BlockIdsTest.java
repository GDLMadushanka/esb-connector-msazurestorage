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

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** The three rules in {@link BlockIds}' javadoc, each of which fails silently if broken. */
public class BlockIdsTest {

    private static final int[] SPREAD = {0, 1, 9, 10, 99, 100, 999, 1_000, 49_999, 50_000, 999_999};

    /** Rule 1. Azure rejects a blob whose blocks have mixed id lengths with 400. */
    @Test
    public void everyIdEncodesToTheSameWidth() {
        BlockIds ids = BlockIds.forJob("job-7");
        for (int index : SPREAD) {
            assertEquals("index " + index, BlockIds.ENCODED_WIDTH, ids.idFor(index).length());
        }
    }

    /**
     * Rule 2, stated as the trap it is. Azure returns the uncommitted list alphabetically, but Base64
     * is not order-preserving, so that order is useless to us. This test exists to keep the mistake
     * from being made again: it asserts the encoded order is NOT index order, which is exactly why the
     * scan decodes instead of sorting.
     */
    @Test
    public void encodedOrderIsNotIndexOrderSoTheScanMustDecode() {
        BlockIds ids = BlockIds.forJob("job-7");
        List<String> encoded = new ArrayList<>();
        for (int index : SPREAD) {
            encoded.add(ids.idFor(index));
        }
        List<String> sorted = new ArrayList<>(encoded);
        sorted.sort(String::compareTo);
        assertNotEquals("if this ever passes, Base64 changed and the comment above is stale",
                encoded, sorted);

        // What the scan actually relies on: every id decodes back to its index, whatever the order.
        for (String id : sorted) {
            assertTrue("every id must decode", ids.indexOf(id).isPresent());
        }
    }

    /** Rule 3. A fresh tag per attempt would leave the previous attempt's blocks unfindable. */
    @Test
    public void sameJobIdProducesTheSameIdsOnAnotherAttempt() {
        assertEquals(BlockIds.forJob("transfer-42").idFor(1234),
                BlockIds.forJob("transfer-42").idFor(1234));
    }

    @Test
    public void differentJobsDoNotShareIds() {
        assertNotEquals(BlockIds.forJob("transfer-42").idFor(7),
                BlockIds.forJob("transfer-43").idFor(7));
    }

    @Test
    public void indexSurvivesTheRoundTrip() {
        BlockIds ids = BlockIds.forJob("job-7");
        for (int index : SPREAD) {
            OptionalInt back = ids.indexOf(ids.idFor(index));
            assertTrue("index " + index + " did not decode", back.isPresent());
            assertEquals(index, back.getAsInt());
        }
    }

    /** A block staged by another run must be skipped, not counted as part of our prefix. */
    @Test
    public void foreignIdsAreNotClaimed() {
        BlockIds mine = BlockIds.forJob("transfer-42");
        BlockIds theirs = BlockIds.forJob("transfer-43");
        assertFalse(mine.indexOf(theirs.idFor(5)).isPresent());
    }

    @Test
    public void rubbishIsRejectedRatherThanGuessed() {
        BlockIds ids = BlockIds.forJob("job-7");
        assertFalse(ids.indexOf(null).isPresent());
        assertFalse(ids.indexOf("").isPresent());
        assertFalse(ids.indexOf("not base64 at all!!").isPresent());
        assertFalse("wrong length must not decode", ids.indexOf("YWJj").isPresent());
    }

    /** No job id means no resume, so the tag is random to stop two such runs colliding on one blob. */
    @Test
    public void runsWithoutAJobIdDoNotCollide() {
        assertNotEquals(BlockIds.forJob("noop").idFor(0), BlockIds.forJob("noop").idFor(0));
        assertNotEquals(BlockIds.forJob(null).idFor(0), BlockIds.forJob(null).idFor(0));
        assertNotEquals(BlockIds.forJob("  ").idFor(0), BlockIds.forJob("  ").idFor(0));
    }

    @Test
    public void anIndexBeyondTheSchemeIsRefused() {
        BlockIds ids = BlockIds.forJob("job-7");
        for (int bad : new int[]{-1, BlockIds.MAX_INDEX + 1}) {
            try {
                ids.idFor(bad);
                fail("expected rejection of index " + bad);
            } catch (IllegalArgumentException expected) {
                assertTrue(expected.getMessage().contains(String.valueOf(bad)));
            }
        }
    }
}
