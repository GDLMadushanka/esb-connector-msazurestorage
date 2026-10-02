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

/**
 * Choosing a block size, which is a correctness question before it is a tuning one.
 *
 * <p>A block blob is capped at <b>50,000 committed blocks</b>, so the block size sets a hard ceiling
 * on how large a blob can be. The usual SDK default of 4 MiB caps a blob at 195 GiB — it cannot upload
 * a terabyte at all, and it fails only after hours of successful uploading, with an opaque
 * {@code BlockCountExceedsLimit}. Sizes that matter:
 *
 * <pre>
 *     4 MiB  ->    195 GiB      32 MiB  ->   1.53 TiB
 *    16 MiB  ->    781 GiB      64 MiB  ->   3.05 TiB
 *    21 MiB  ->   1.00 TiB     100 MiB  ->   4.77 TiB
 * </pre>
 *
 * <p>So when the source size is known — and {@code identify()} reports it before the run starts — the
 * block size is <b>derived</b> from it rather than defaulted. When it is not known, the floor applies
 * and the ceiling is 1.53 TiB.
 *
 * <p>Deriving against {@link #TARGET_BLOCKS} rather than {@link #MAX_BLOCKS} leaves headroom. Sizing
 * exactly to the limit means a source a single byte larger than reported has nowhere to put its last
 * block, after hours of work; ten percent slack costs nothing and removes the class of failure.
 *
 * <p>Heap is the counterweight: a block is buffered so that a transient failure can be retried in
 * place rather than unwinding the run, and a parallel upload holds one buffer per uploader. The cost
 * is {@code blockSize x uploadConcurrency} per transfer, against MI's default {@code -Xmx1024m},
 * which is why the floor is 32 MiB and not higher.
 */
public final class BlockSizing {

    private static final long MIB = 1024L * 1024L;

    /** Azure rejects a smaller block on any but the final one; also far too chatty to be useful. */
    public static final long MIN_BLOCK = MIB;

    /** Azure's hard limit per block for service version 2019-12-12 and later. */
    public static final long MAX_BLOCK = 4000L * MIB;

    /** Used when the source size is unknown. Ceiling 1.53 TiB, 32 MiB of heap per uploader. */
    public static final long DEFAULT_FLOOR = 32L * MIB;

    /** Azure's ceiling on committed blocks in one blob. Exceeding it fails the commit outright. */
    public static final int MAX_BLOCKS = 50_000;

    /** Derive against this, not {@link #MAX_BLOCKS}, so a slightly larger source still fits. */
    public static final int TARGET_BLOCKS = 45_000;

    private BlockSizing() {
    }

    /**
     * The block size to use when the deployer configured none.
     *
     * @param sourceSizeBytes source size if known, else a non-positive value
     * @return {@link #DEFAULT_FLOOR}, or larger when the source needs bigger blocks to fit
     * @throws IllegalArgumentException if no legal block size can hold this source
     */
    public static long derive(long sourceSizeBytes) {
        if (sourceSizeBytes <= 0) {
            return DEFAULT_FLOOR;
        }
        long needed = ceilToMib(divideRoundingUp(sourceSizeBytes, TARGET_BLOCKS));
        long chosen = Math.max(DEFAULT_FLOOR, needed);
        if (chosen > MAX_BLOCK) {
            throw new IllegalArgumentException(
                    "a source of " + sourceSizeBytes + " bytes cannot be uploaded as a block blob: it"
                            + " would need blocks of " + chosen / MIB + " MiB and Azure's limit is "
                            + MAX_BLOCK / MIB + " MiB. The maximum block blob is about 190 TiB.");
        }
        return chosen;
    }

    /**
     * Rejects a configured block size that cannot hold the source, <b>before</b> the upload starts.
     *
     * <p>The message names the size that would have worked, because the alternative is discovering the
     * problem as a 409 from Azure after hours of successful uploading.
     *
     * @param blockSize       the configured size
     * @param sourceSizeBytes source size if known, else a non-positive value, in which case the
     *                        source cannot be checked against and only the block size itself is
     */
    public static void validate(long blockSize, long sourceSizeBytes) {
        if (blockSize < MIN_BLOCK || blockSize > MAX_BLOCK) {
            throw new IllegalArgumentException(
                    "blockSize must be between " + MIN_BLOCK / MIB + " MiB and " + MAX_BLOCK / MIB
                            + " MiB, but was " + blockSize + " bytes");
        }
        if (sourceSizeBytes <= 0) {
            return;
        }
        long blocks = divideRoundingUp(sourceSizeBytes, blockSize);
        if (blocks > MAX_BLOCKS) {
            throw new IllegalArgumentException(
                    "a blockSize of " + blockSize / MIB + " MiB would need " + blocks + " blocks for a"
                            + " source of " + sourceSizeBytes + " bytes, and Azure allows " + MAX_BLOCKS
                            + ". Use at least " + smallestThatFits(sourceSizeBytes) / MIB + " MiB.");
        }
    }

    /**
     * The runtime guard, for the case {@link #validate} could not see: the source size was unknown, so
     * the block count is only discovered while uploading.
     *
     * @param index the block about to be staged
     * @throws IllegalStateException when the blob has run out of blocks
     */
    public static void checkIndex(int index, long blockSize, long bytesSoFar) {
        if (index >= MAX_BLOCKS) {
            long atLeast = ceilToMib(divideRoundingUp(bytesSoFar, TARGET_BLOCKS));
            throw new IllegalStateException(
                    "reached Azure's limit of " + MAX_BLOCKS + " blocks after " + bytesSoFar
                            + " bytes at a blockSize of " + blockSize / MIB + " MiB. The source is"
                            + " larger than this block size can hold; configure blockSize to at least "
                            + Math.max(atLeast, blockSize + MIB) / MIB + " MiB and run it again.");
        }
    }

    /** The smallest whole-MiB block size that keeps this source inside {@link #MAX_BLOCKS}. */
    public static long smallestThatFits(long sourceSizeBytes) {
        return ceilToMib(divideRoundingUp(sourceSizeBytes, MAX_BLOCKS));
    }

    /**
     * The largest blob a given block size can hold — the number a deployer actually needs, because the
     * block size on its own does not look like a limit until the upload fails at 50,000 blocks.
     */
    public static long maxBlobFor(long blockSize) {
        return blockSize * MAX_BLOCKS;
    }

    /** How many blocks a source of this size takes at this block size. */
    public static long blocksFor(long sourceSizeBytes, long blockSize) {
        return sourceSizeBytes <= 0 ? 0 : divideRoundingUp(sourceSizeBytes, blockSize);
    }

    /**
     * A byte count as a person reads it — "32 MiB", "1.53 TiB". Binary units throughout, because every
     * limit on this page is binary and mixing the two is how 4000MB becomes a rejected block size.
     */
    public static String describe(long bytes) {
        if (bytes < MIB) {
            return bytes + " bytes";
        }
        String[] units = {"MiB", "GiB", "TiB"};
        double value = (double) bytes / MIB;
        int unit = 0;
        while (value >= 1024 && unit < units.length - 1) {
            value /= 1024;
            unit++;
        }
        String number = value >= 100 || value == Math.floor(value)
                ? String.valueOf(Math.round(value))
                : String.format(java.util.Locale.ROOT, "%.2f", value);
        return number + " " + units[unit];
    }

    private static long divideRoundingUp(long value, long divisor) {
        return (value + divisor - 1) / divisor;
    }

    private static long ceilToMib(long bytes) {
        return Math.max(MIB, divideRoundingUp(bytes, MIB) * MIB);
    }
}
