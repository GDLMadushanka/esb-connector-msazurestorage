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

import com.azure.storage.blob.models.Block;
import com.azure.storage.blob.models.BlockList;
import com.azure.storage.blob.models.BlockListType;
import com.azure.storage.blob.specialized.BlockBlobClient;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.OptionalInt;
import java.util.Set;

/**
 * What the destination really holds, which is the other half of resume.
 *
 * <p>The checkpoint is the authority on how far this transfer got, but it is written <i>after</i> the
 * work it describes, so it is always at or behind reality. This asks Azure the same question from the
 * other side, and the two answers together decide where the next attempt starts.
 *
 * <p>Azure makes it possible by keeping <b>uncommitted blocks for a week</b> and offering
 * {@code Get Block List?blocklisttype=uncommitted}, which its own documentation describes as the way
 * "to determine which blocks are missing from the blob in cases where calls to Put Block or Put Block
 * List have failed". Those blocks are invisible to any reader until {@code Put Block List} runs, so
 * nothing is published by having them there.
 *
 * <h2>Contiguous, never merely highest</h2>
 * A parallel upload can leave block 12003 on the service while 12002 never landed. Counting the
 * highest present block would claim a prefix with a hole in it, and the commit would produce a blob
 * that is the right length and silently wrong. So the scan walks up from zero and stops at the first
 * gap — the same rule {@link UploadWatermark} applies in memory during the run, applied here to
 * Azure's list instead.
 *
 * <h2>Why it decodes rather than sorts</h2>
 * Azure returns the uncommitted list alphabetically, which looks like it could be walked directly. It
 * cannot: the ids are Base64 and <b>Base64 does not preserve order</b>. Every name is decoded through
 * {@link BlockIds#indexOf(String)} instead, which also discards blocks belonging to another run.
 *
 * <p>This is deliberately shaped as the pair a future framework-level facade would ask any sink for —
 * {@code durablePosition()} and {@code discardAbove()} — so that lifting it out later moves a class
 * rather than rewriting the logic.
 */
public final class BlockPrefix {

    private BlockPrefix() {
    }

    /**
     * The contiguous prefix, in blocks, that these names prove is on the service.
     *
     * <p>Pure, so it can be tested without an Azure account: the interesting cases are gaps, foreign
     * blocks and duplicates, none of which need a network.
     *
     * @param blockNames every id Azure reported, committed and uncommitted alike
     * @param ids        the naming scheme for <i>this</i> run; anything else is ignored
     */
    public static int contiguousPrefix(Collection<String> blockNames, BlockIds ids) {
        return staged(blockNames, ids).prefix();
    }

    /**
     * Everything these names prove is on the service, and the contiguous prefix of it.
     *
     * <p>The prefix alone decides where the transfer <i>restarts</i>, because that is the only shape a
     * single resume offset can describe and the only one a seekable source can be told to honour. The
     * full set decides what gets <i>re-sent</i>: a block above the gap is already on the service, so
     * staging it again spends WAN bandwidth to produce a byte-identical result.
     *
     * <p>Both come from one walk, because they come from one {@code listBlocks} call and splitting
     * them into two accessors would mean asking Azure the same question twice per resumed run.
     */
    public static Staged staged(Collection<String> blockNames, BlockIds ids) {
        Set<Integer> present = new HashSet<>();
        for (String name : blockNames) {
            OptionalInt index = ids.indexOf(name);
            if (index.isPresent()) {
                present.add(index.getAsInt());
            }
        }
        int prefix = 0;
        while (present.contains(prefix)) {
            prefix++;
        }
        return new Staged(present, prefix);
    }

    /**
     * What Azure already holds for this run.
     *
     * @param present every block index the service reported, committed or uncommitted
     * @param prefix  how far an unbroken run from block 0 reaches — always {@code <=} every gap
     */
    public record Staged(Set<Integer> present, int prefix) {

        /** An empty answer, for a first attempt that has nothing to reconcile against. */
        public static final Staged NONE = new Staged(Set.of(), 0);

        /**
         * Whether this block can be left alone.
         *
         * <p>Only asked about blocks at or above the resume point; everything below it was never read.
         * Safe because the id is derived from the run and the byte range, so a block with this id
         * holds these bytes by construction — the same assumption the prefix skip already makes, and
         * the one {@code sourceIdentity="strict"} exists to protect.
         */
        public boolean alreadyHas(int index) {
            return present.contains(index);
        }

        /** How many blocks sit above the prefix — the ones a gap would otherwise force us to re-send. */
        public int strandedAbove() {
            return (int) present.stream().filter(i -> i > prefix).count();
        }
    }

    /**
     * How far the blob really got, in blocks — the sink's half of the facade contract.
     *
     * <p>Asks for both lists. A committed blob whose blocks are ours means a previous attempt
     * succeeded and this one is redundant, which the caller decides what to do about; the common case
     * is an uncommitted prefix and no committed blocks at all.
     */
    public static int durablePosition(BlockBlobClient client, BlockIds ids) {
        return stagedOn(client, ids).prefix();
    }

    /** One {@code listBlocks} call, answering both questions. See {@link #staged}. */
    public static Staged stagedOn(BlockBlobClient client, BlockIds ids) {
        BlockList lists;
        try {
            lists = client.listBlocks(BlockListType.ALL);
        } catch (RuntimeException notThereYet) {
            // No blob and no staged blocks is a 404, and it is the ordinary first-run answer rather
            // than a failure. Treating it as "nothing durable" is exactly right.
            return Staged.NONE;
        }
        List<String> names = new ArrayList<>();
        for (Block block : lists.getCommittedBlocks()) {
            names.add(block.getName());
        }
        for (Block block : lists.getUncommittedBlocks()) {
            names.add(block.getName());
        }
        return staged(names, ids);
    }

    /**
     * Cut the destination back to {@code position} blocks — the other half of the facade contract, and
     * for Azure a no-op.
     *
     * <p>Nothing has to be undone: a block above the resume point is uncommitted, so no reader can see
     * it, and {@code Put Block List} simply will not name it. Azure garbage-collects it a week after
     * the last write. A file sink truncates here; this one has nothing to truncate, and that asymmetry
     * is the reason the contract is two methods rather than one.
     */
    public static void discardAbove(int position) {
        // Intentionally empty. See the javadoc: uncommitted blocks above the resume point are
        // invisible, unreferenced by the commit, and reclaimed by Azure on their own.
    }
}
