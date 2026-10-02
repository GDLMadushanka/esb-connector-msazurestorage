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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Block size decides the maximum blob, so getting it wrong fails only after hours of successful
 * uploading. These assert the contract rather than particular numbers.
 */
public class BlockSizingTest {

    private static final long MIB = 1024L * 1024L;
    private static final long GIB = 1024L * MIB;
    private static final long TIB = 1024L * GIB;

    private static long blocksFor(long size, long blockSize) {
        return (size + blockSize - 1) / blockSize;
    }

    @Test
    public void anUnknownSourceSizeFallsBackToTheFloor() {
        assertEquals(BlockSizing.DEFAULT_FLOOR, BlockSizing.derive(0));
        assertEquals(BlockSizing.DEFAULT_FLOOR, BlockSizing.derive(-1));
    }

    /** The contract: whatever we derive must fit the source inside Azure's 50,000 blocks. */
    @Test
    public void aDerivedSizeAlwaysFitsInsideAzuresBlockLimit() {
        long[] sizes = {1, MIB, GIB, 100 * GIB, 500 * GIB, TIB, 5 * TIB, 50 * TIB, 150 * TIB};
        for (long size : sizes) {
            long block = BlockSizing.derive(size);
            long blocks = blocksFor(size, block);
            assertTrue(size + " bytes needed " + blocks + " blocks", blocks <= BlockSizing.MAX_BLOCKS);
            assertTrue("below the floor for " + size, block >= BlockSizing.DEFAULT_FLOOR);
            assertTrue("above Azure's block limit for " + size, block <= BlockSizing.MAX_BLOCK);
            BlockSizing.validate(block, size);
        }
    }

    /** The headroom the javadoc promises: a source larger than reported still has somewhere to go. */
    @Test
    public void aDerivedSizeLeavesHeadroomAboveTheReportedSize() {
        for (long size : new long[]{500 * GIB, TIB, 5 * TIB}) {
            long block = BlockSizing.derive(size);
            assertTrue("no headroom at " + size,
                    blocksFor(size, block) <= BlockSizing.TARGET_BLOCKS);
        }
    }

    /** The reason this class exists: the usual 4 MiB default cannot upload a terabyte. */
    @Test
    public void theUsualFourMebibyteDefaultIsRejectedForATerabyte() {
        try {
            BlockSizing.validate(4 * MIB, TIB);
            fail("4 MiB should not be accepted for a 1 TiB source");
        } catch (IllegalArgumentException expected) {
            assertTrue("the message must name a size that would work",
                    expected.getMessage().contains(String.valueOf(BlockSizing.smallestThatFits(TIB) / MIB)));
        }
    }

    @Test
    public void smallestThatFitsIsTheSmallestThatFits() {
        for (long size : new long[]{100 * GIB, 500 * GIB, TIB, 5 * TIB}) {
            long smallest = BlockSizing.smallestThatFits(size);
            assertTrue("does not fit at " + size, blocksFor(size, smallest) <= BlockSizing.MAX_BLOCKS);
            assertTrue("not minimal at " + size,
                    blocksFor(size, smallest - MIB) > BlockSizing.MAX_BLOCKS);
        }
    }

    @Test
    public void blockSizesOutsideAzuresRangeAreRefused() {
        for (long bad : new long[]{0, BlockSizing.MIN_BLOCK - 1, BlockSizing.MAX_BLOCK + 1}) {
            try {
                BlockSizing.validate(bad, 0);
                fail("expected rejection of block size " + bad);
            } catch (IllegalArgumentException expected) {
                assertTrue(expected.getMessage().contains("blockSize"));
            }
        }
    }

    /** With an unknown source size, the limit can only be found while uploading. */
    @Test
    public void theRuntimeGuardFiresAtAzuresLimitAndNamesAWorkingSize() {
        long block = 1 * MIB;
        long bytesSoFar = BlockSizing.MAX_BLOCKS * block;
        BlockSizing.checkIndex(BlockSizing.MAX_BLOCKS - 1, block, bytesSoFar);
        try {
            BlockSizing.checkIndex(BlockSizing.MAX_BLOCKS, block, bytesSoFar);
            fail("expected the 50,000 block guard to fire");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage().contains(String.valueOf(BlockSizing.MAX_BLOCKS)));
            assertTrue("must suggest a larger block size",
                    expected.getMessage().contains("configure blockSize"));
        }
    }

    /** The numbers that go into the log line, which is the only place a deployer sees the ceiling. */
    @Test
    public void describesSizesTheWayTheLogLineNeeds() {
        assertEquals("32 MiB", BlockSizing.describe(32 * MIB));
        assertEquals("195 GiB", BlockSizing.describe(BlockSizing.maxBlobFor(4 * MIB)));
        assertEquals("1.53 TiB", BlockSizing.describe(BlockSizing.maxBlobFor(32 * MIB)));
        assertEquals("512 bytes", BlockSizing.describe(512));
    }

    @Test
    public void maxBlobForIsTheCeilingThatBlockSizeImplies() {
        assertEquals(BlockSizing.MAX_BLOCKS * 4 * MIB, BlockSizing.maxBlobFor(4 * MIB));
        assertTrue("4 MiB must not reach a terabyte", BlockSizing.maxBlobFor(4 * MIB) < TIB);
        assertTrue("32 MiB must comfortably pass a terabyte", BlockSizing.maxBlobFor(32 * MIB) > TIB);
        assertEquals(6, BlockSizing.blocksFor(21_013_905L, 4 * MIB));
        assertEquals(0, BlockSizing.blocksFor(0, 4 * MIB));
    }

    @Test
    public void aSourceTooLargeForAnyBlockSizeIsRefusedUpFront() {
        try {
            BlockSizing.derive(300 * TIB);
            fail("300 TiB exceeds the block blob maximum and should be refused");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("190 TiB"));
        }
    }
}
