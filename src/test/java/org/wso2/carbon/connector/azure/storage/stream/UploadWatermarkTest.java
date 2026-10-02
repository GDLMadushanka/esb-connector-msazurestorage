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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * The checkpoint carries one number meaning "everything below this is durable". These are the cases
 * where a naive "highest completed block" would put a hole underneath it.
 */
public class UploadWatermarkTest {

    /** The exact sequence in {@link UploadWatermark}'s javadoc, so the two cannot drift apart. */
    @Test
    public void advancesOnlyOverContiguousCompletions() {
        UploadWatermark w = new UploadWatermark(12_000);
        assertEquals(12_000, w.watermark());

        assertEquals("12001 alone leaves 12000 as a hole", 12_000, w.completed(12_001));
        assertEquals("12003 too", 12_000, w.completed(12_003));
        assertEquals("12000 releases 12001 as well", 12_002, w.completed(12_000));
        assertEquals("12002 releases 12003 as well", 12_004, w.completed(12_002));
        assertEquals(0, w.strandedAbove());
    }

    @Test
    public void aRetriedBlockCannotPushThePositionPastAGap() {
        UploadWatermark w = new UploadWatermark(0);
        w.completed(1);
        assertEquals(0, w.completed(1));
        assertEquals(0, w.completed(1));
        assertEquals("only block 0 can release block 1", 2, w.completed(0));
    }

    @Test
    public void completionsBelowTheWatermarkAreIgnored() {
        UploadWatermark w = new UploadWatermark(500);
        assertEquals(500, w.completed(12));
        assertEquals(501, w.completed(500));
    }

    @Test
    public void strandedCompletionsAreCountedWhileTheyWait() {
        UploadWatermark w = new UploadWatermark(0);
        w.completed(3);
        w.completed(2);
        assertEquals(2, w.strandedAbove());
        w.completed(1);
        w.completed(0);
        assertEquals(4, w.watermark());
        assertEquals(0, w.strandedAbove());
    }

    /** Arrival order must not change the answer, since uploader threads finish in any order. */
    @Test
    public void anyArrivalOrderReachesTheSameWatermark() {
        List<Integer> blocks = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            blocks.add(i);
        }
        for (int seed = 0; seed < 25; seed++) {
            Collections.shuffle(blocks, new java.util.Random(seed));
            UploadWatermark w = new UploadWatermark(0);
            for (int index : blocks) {
                w.completed(index);
            }
            assertEquals("seed " + seed, 200, w.watermark());
            assertEquals("seed " + seed, 0, w.strandedAbove());
        }
    }

    /** Uploader threads call completed(); the consume thread reads watermark(). */
    @Test
    public void isSafeUnderConcurrentCompletion() throws Exception {
        int blocks = 500;
        UploadWatermark w = new UploadWatermark(0);
        ExecutorService pool = Executors.newFixedThreadPool(8);
        CountDownLatch go = new CountDownLatch(1);
        try {
            for (int i = 0; i < blocks; i++) {
                final int index = i;
                pool.submit(() -> {
                    go.await();
                    w.completed(index);
                    return null;
                });
            }
            go.countDown();
            pool.shutdown();
            assertTrue("pool did not drain", pool.awaitTermination(30, TimeUnit.SECONDS));
        } finally {
            pool.shutdownNow();
        }
        assertEquals(blocks, w.watermark());
        assertEquals(0, w.strandedAbove());
    }
}
