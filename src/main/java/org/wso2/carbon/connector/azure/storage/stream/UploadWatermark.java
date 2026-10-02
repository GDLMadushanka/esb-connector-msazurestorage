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

import java.util.SortedSet;
import java.util.TreeSet;

/**
 * How far a parallel upload may honestly claim to have got.
 *
 * <p>A checkpoint carries one number, and that number means <b>everything below this position is
 * durable</b>. Resume leans on exactly that: restarting at N asserts every block below N is on the
 * service. So with several blocks in flight and finishing out of order, the position may only advance
 * to the highest index with <i>no gap underneath it</i> — never to the highest index that happens to
 * have finished.
 *
 * <pre>
 *   in flight:  12000   12001   12002   12003        claimable
 *   ---------------------------------------------------------
 *   start         .       .       .       .            12000
 *   12001 lands   .     done      .       .            12000   still 12000: 12000 is a hole
 *   12003 lands   .     done      .     done           12000
 *   12000 lands  done   done      .     done           12002   jumps by two
 *   12002 lands  done   done    done    done           12004   jumps by two
 * </pre>
 *
 * <p>Parallelism therefore does not weaken the single-checkpoint model; it makes the number lag and
 * then jump. Nothing above the watermark is wasted either — those blocks are on the service, and the
 * resume scan over the uncommitted block list finds them by running <i>this same walk</i> against
 * Azure's list instead of against memory.
 *
 * <p><b>Bounded by concurrency, not by file size.</b> Only completions above the watermark are held,
 * and the buffer pool admits at most {@code uploadConcurrency} blocks at once, so this set never holds
 * more than a handful of entries however many blocks the transfer has.
 *
 * <p>Uploader threads call {@link #completed(int)}; the {@code consume()} thread reads
 * {@link #watermark()} and is the only caller of {@code unitDone}. One writer to the checkpoint keeps
 * its "never goes backwards" contract trivially true.
 */
public final class UploadWatermark {

    private final SortedSet<Integer> doneAbove = new TreeSet<>();
    private int watermark;

    /**
     * @param resumeFrom the first block this attempt will upload; blocks below it are already durable
     */
    public UploadWatermark(int resumeFrom) {
        if (resumeFrom < 0) {
            throw new IllegalArgumentException("resume block cannot be negative: " + resumeFrom);
        }
        this.watermark = resumeFrom;
    }

    /**
     * Records that block {@code index} is durable on the service, and walks the watermark forward over
     * any completions that were waiting on it.
     *
     * <p>Idempotent: a block already below the watermark, or already recorded, changes nothing. A
     * retried block therefore cannot push the position past a gap.
     *
     * @return the watermark after this completion — the number of blocks now safe to claim
     */
    public synchronized int completed(int index) {
        if (index < 0) {
            throw new IllegalArgumentException("block index cannot be negative: " + index);
        }
        if (index < watermark) {
            return watermark;
        }
        doneAbove.add(index);
        while (doneAbove.remove(watermark)) {
            watermark++;
        }
        return watermark;
    }

    /** Blocks {@code 0 .. watermark-1} are durable. This is the only number the checkpoint may hold. */
    public synchronized int watermark() {
        return watermark;
    }

    /**
     * Completions sitting above the watermark, waiting on a gap. Never larger than the upload
     * concurrency; useful in a log line when a slow block is holding the checkpoint back.
     */
    public synchronized int strandedAbove() {
        return doneAbove.size();
    }
}
