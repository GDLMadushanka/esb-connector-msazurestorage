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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * What Azure's block list proves is really there. The cases that matter are the ones where counting
 * blocks instead of walking them would commit a blob with a hole in it.
 */
public class BlockPrefixTest {

    private final BlockIds ids = BlockIds.forJob("transfer-42");

    private List<String> staged(int... indices) {
        List<String> names = new ArrayList<>();
        for (int index : indices) {
            names.add(ids.idFor(index));
        }
        return names;
    }

    @Test
    public void nothingStagedIsNothingDurable() {
        assertEquals(0, BlockPrefix.contiguousPrefix(Collections.emptyList(), ids));
    }

    @Test
    public void anUnbrokenRunCountsInFull() {
        assertEquals(5, BlockPrefix.contiguousPrefix(staged(0, 1, 2, 3, 4), ids));
    }

    /** The case a parallel upload creates, and the reason this walks rather than counts. */
    @Test
    public void stopsAtTheFirstGapHoweverManyBlocksLieBeyondIt() {
        assertEquals("12002 is missing, so only 0..12001 are provable",
                2, BlockPrefix.contiguousPrefix(staged(0, 1, 3, 4, 5), ids));
        assertEquals("a gap at zero proves nothing at all",
                0, BlockPrefix.contiguousPrefix(staged(1, 2, 3), ids));
    }

    /** Azure returns the uncommitted list alphabetically, which for Base64 ids is not index order. */
    @Test
    public void theOrderTheNamesArriveInDoesNotMatter() {
        List<String> shuffled = staged(0, 1, 2, 3, 4);
        Collections.sort(shuffled);
        assertEquals(5, BlockPrefix.contiguousPrefix(shuffled, ids));
        Collections.reverse(shuffled);
        assertEquals(5, BlockPrefix.contiguousPrefix(shuffled, ids));
    }

    /** Another run's blocks sit in the same list and must not be counted as our prefix. */
    @Test
    public void blocksBelongingToAnotherRunAreIgnored() {
        BlockIds other = BlockIds.forJob("transfer-43");
        List<String> mixed = new ArrayList<>(staged(0, 1));
        mixed.addAll(Arrays.asList(other.idFor(2), other.idFor(3), "c29tZXRoaW5nIGVsc2U="));
        assertEquals("their 2 and 3 must not extend our prefix",
                2, BlockPrefix.contiguousPrefix(mixed, ids));
    }

    @Test
    public void aDuplicatedBlockIsStillOneBlock() {
        List<String> withDuplicates = new ArrayList<>(staged(0, 1, 2));
        withDuplicates.addAll(staged(1, 2));
        assertEquals(3, BlockPrefix.contiguousPrefix(withDuplicates, ids));
    }

    /** discardAbove is a no-op for Azure; it exists so the contract is the same for every sink. */
    @Test
    public void discardingAboveThePrefixDoesNothingAndDoesNotThrow() {
        BlockPrefix.discardAbove(0);
        BlockPrefix.discardAbove(12_000);
    }

    // ------------------------------------------------- the set above the prefix

    /**
     * The prefix says where to restart; the set says what not to re-send. They are different answers
     * and a resumed parallel upload needs both — the prefix stops at the first gap, and everything
     * above it is on the service all the same.
     */
    @Test
    public void theSetKeepsWhatThePrefixHasToStopAt() {
        BlockPrefix.Staged s = BlockPrefix.staged(staged(0, 1, 3, 4, 5), ids);

        assertEquals("restart is still bounded by the gap at 2", 2, s.prefix());
        assertTrue("but 3, 4 and 5 are provably on the service", s.alreadyHas(3));
        assertTrue(s.alreadyHas(4));
        assertTrue(s.alreadyHas(5));
        assertFalse("the gap itself must be sent", s.alreadyHas(2));
        assertEquals("three blocks that would otherwise be uploaded twice", 3, s.strandedAbove());
    }

    /** With no gap there is nothing stranded, and the prefix already covers everything. */
    @Test
    public void anUnbrokenRunStrandsNothing() {
        BlockPrefix.Staged s = BlockPrefix.staged(staged(0, 1, 2, 3), ids);

        assertEquals(4, s.prefix());
        assertEquals(0, s.strandedAbove());
        assertFalse("nothing is claimed beyond what was reported", s.alreadyHas(4));
    }

    /** A first attempt has nothing to reconcile against, and must not claim it has. */
    @Test
    public void theEmptyAnswerClaimsNothing() {
        assertEquals(0, BlockPrefix.Staged.NONE.prefix());
        assertEquals(0, BlockPrefix.Staged.NONE.strandedAbove());
        assertFalse(BlockPrefix.Staged.NONE.alreadyHas(0));
    }

    /**
     * Another run's blocks must not make this one skip an upload. They are already dropped from the
     * prefix walk; the set has to drop them too, or a resumed transfer would leave a block out of its
     * own blob because somebody else's block happened to sit at that index.
     */
    @Test
    public void anotherRunsBlocksAreNotMistakenForOurs() {
        BlockIds other = BlockIds.forJob("transfer-43");
        List<String> names = new ArrayList<>(staged(0, 1));
        names.add(other.idFor(5));

        BlockPrefix.Staged s = BlockPrefix.staged(names, ids);

        assertEquals(2, s.prefix());
        assertFalse("block 5 belongs to another run and proves nothing here", s.alreadyHas(5));
        assertEquals(0, s.strandedAbove());
    }

    /** contiguousPrefix is now a view over the same walk, so the two can never disagree. */
    @Test
    public void thePrefixAccessorAgreesWithTheSet() {
        List<String> names = new ArrayList<>(staged(0, 1, 2, 7, 9));
        assertEquals(BlockPrefix.staged(names, ids).prefix(),
                BlockPrefix.contiguousPrefix(names, ids));
    }
}
