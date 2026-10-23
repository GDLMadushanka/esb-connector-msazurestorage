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

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.core.util.Context;
import com.azure.storage.blob.specialized.BlockBlobClient;
import org.apache.synapse.MessageContext;
import org.apache.synapse.stream.StageArtifact;
import org.apache.synapse.stream.StageDestination;
import org.apache.synapse.stream.StreamContext;
import org.apache.synapse.stream.StreamException;
import org.apache.synapse.stream.StreamSink;
import org.wso2.carbon.connector.azure.storage.connection.AzureStorageConnectionHandler;
import org.wso2.carbon.connector.azure.storage.util.AzureConstants;
import org.wso2.carbon.connector.azure.storage.util.AzureUtil;
import org.wso2.carbon.connector.core.AbstractConnector;
import org.wso2.carbon.connector.core.ConnectException;
import org.wso2.carbon.connector.core.connection.ConnectionHandler;
import org.wso2.carbon.connector.core.util.ConnectorUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Writes a byte stream to an Azure block blob, in blocks, restartably.
 *
 * <pre>
 *   &lt;azure.blobStreamWrite name="upload"&gt;
 *     &lt;container&gt;inbox&lt;/container&gt;
 *     &lt;blob&gt;reports/out.jsonl&lt;/blob&gt;
 *     &lt;blockSize&gt;32MB&lt;/blockSize&gt;
 *   &lt;/azure.blobStreamWrite&gt;
 * </pre>
 *
 * <h2>Why not the SDK's own uploader</h2>
 * {@code BlockBlobClient.getBlobOutputStream(ParallelTransferOptions, ...)} would do the chunking, the
 * parallelism and the retrying, and reduce this class to about ten lines. It is unusable for a
 * <i>resumable</i> transfer for two independent reasons. There is no point at which it reports that a
 * particular byte range became durable, so there is nothing on which to key a checkpoint — the
 * ordering contract is not merely hard to honour, it is unobservable. And it names blocks with
 * {@code UUID.randomUUID()}, so after a crash the staged blocks cannot be matched back to byte ranges
 * and the uploaded prefix is unrecoverable. See {@link BlockIds}.
 *
 * <h2>A failed run publishes nothing, for free</h2>
 * Staged blocks are invisible to every reader until {@code Put Block List} runs, so the blob does not
 * exist until the run succeeds. The commit is registered with the scope and runs only on the success
 * path; {@code abort()} does nothing at all and Azure reclaims the orphans after a week. This is a
 * cleaner guarantee than the temp-name-and-rename a file sink has to perform, and it is why block
 * blobs are used here rather than append blobs, whose appends are visible immediately.
 *
 * <h2>The ordering contract</h2>
 * Step one is this operator's and only this operator's: <b>do not advance the checkpoint until Azure
 * has acknowledged the block</b>. {@code unitDone} fuses the remaining steps.
 *
 * <p>Position is counted in <b>blocks</b> ({@code "blocks"}), not bytes, because a block
 * is the unit of work here — and because a {@code checkpointFrequency} of 1 should mean "at most one
 * block re-uploaded", which is a number a deployer can reason about, rather than one byte, which is
 * not.
 *
 * <p>Stateless: one instance serves every concurrent run, so everything below lives in locals.
 */
public class AzureBlobStreamWrite extends AbstractConnector implements StreamSink {

    private static final String NAME = "azure.blobStreamWrite";

    /**
     * Versions the meaning of the checkpoint payload, not the operator. The payload is the block size
     * the attempt actually used, as a decimal byte count.
     */
    private static final String FORMAT_VERSION = "azure-block-1";

    /** How often the reader looks up while waiting on a buffer, to notice a failure or a stop. */
    private static final long BUFFER_POLL_MILLIS = 100L;

    /** How often to record a checkpoint advance. Every block would be noise on a large transfer. */
    private static final int CHECKPOINT_LOG_EVERY = 10;

    private static final String[] PARAMETERS = {"container", "blob", "blockSize", "contentType",
            "overwrite", "uploadConcurrency", "verifyBlocks"};

    @Override
    public void connect(MessageContext messageContext) throws ConnectException {
        throw new ConnectException(NAME + " is only valid inside a <streamSequence>. It consumes a byte"
                + " chain rather than mediating a message, so there is nothing for it to do in a"
                + " sequence. Use msazurestorage.uploadBlob instead.");
    }

    @Override
    public String name() {
        return NAME;
    }

    /** Keeps a resume position, in blocks, and writes no artifact. Legal because a sink is last. */
    @Override
    public boolean checkpointed() {
        return true;
    }

    /** Runs the real parse over literal parameters at deployment, so it cannot drift from runtime. */
    @Override
    public void validateConfiguration(Map<String, String> literalParameters) throws StreamException {
        if (literalParameters == null || literalParameters.isEmpty()) {
            return;
        }
        try {
            AzureBlobSettings.from(literalParameters, NAME);
        } catch (IllegalArgumentException invalid) {
            throw new StreamException(invalid.getMessage(), invalid, false);
        }
    }

    /**
     * Where this sink will really resume, in bytes, so the framework can start the source there.
     *
     * <p>Answered before the source opens, which is why it does its own reconciliation rather than
     * trusting the checkpoint: the contract asks for the position this sink will <i>actually</i> write
     * from, and a number larger than that leaves a hole nothing downstream could detect. The cost is
     * one extra {@code listBlocks} per resumed run, against re-reading the whole prefix.
     *
     * <p>Returns 0 for a fresh run — start at the beginning — and -1 only if the block size the
     * earlier attempt used cannot be recovered, in which case counting blocks tells us nothing about
     * bytes and the honest answer is "no opinion".
     */
    @Override
    public long durableByteOffset(StreamContext ctx) throws IOException {
        AzureBlobSettings settings;
        try {
            settings = settings(ctx);
        } catch (StreamException unreadable) {
            return -1L;
        }
        StageArtifact checkpoint = checkpoint(ctx, settings);
        int claimed = (int) checkpoint.resumePosition();
        if (claimed == 0) {
            return 0L;
        }
        String recorded = checkpoint.resumePayload();
        if (recorded == null) {
            return -1L;
        }
        long blockSize;
        try {
            blockSize = Long.parseLong(recorded.trim());
        } catch (NumberFormatException unreadable) {
            return -1L;
        }
        try {
            BlockBlobClient client = blockBlobClient(ctx, settings);
            BlockIds ids = BlockIds.forJob(ctx.job().jobId());
            BlockPrefix.Staged staged = BlockPrefix.stagedOn(client, ids);
            // Asked before the run, so nothing is discarded: this answers where the run will start,
            // and must not alter the destination as a side effect of being asked.
            return checkpoint.resumeAgainst(new StagedBlocks(staged), false) * blockSize;
        } catch (StreamException unreachable) {
            // Not fatal. Not knowing costs a re-read, which is slow and correct.
            return -1L;
        }
    }

    /**
     * This stage's checkpoint, with the deployer's cadence.
     *
     * <p>One method rather than two call sites: {@code StreamContext.artifact} caches by name, so a
     * second call passing a different frequency would be silently ignored in favour of the first,
     * and which call ran first depends on whether the framework asked for a resume offset. Reading
     * it from the settings in one place removes the question.
     */
    private static StageArtifact checkpoint(StreamContext ctx, AzureBlobSettings settings) {
        return ctx.artifact("blocks", FORMAT_VERSION, settings.checkpointFrequency());
    }

    @Override
    public void consume(InputStream in, StreamContext ctx) throws StreamException, IOException {
        AzureBlobSettings settings = settings(ctx);
        BlockBlobClient client = blockBlobClient(ctx, settings);
        BlockIds ids = BlockIds.forJob(ctx.job().jobId());

        StageArtifact checkpoint = checkpoint(ctx, settings);
        Long expected = ctx.job().expectedSize();
        long sourceSize = expected == null ? 0L : expected;
        long blockSize = blockSize(ctx, settings, checkpoint, sourceSize);

        int claimed = (int) checkpoint.resumePosition();
        // One call, two answers: where to restart, and which blocks above that point are already
        // there. A fresh run asks nothing — there is nothing on the service to reconcile against.
        BlockPrefix.Staged staged = claimed == 0
                ? BlockPrefix.Staged.NONE
                : BlockPrefix.stagedOn(client, ids);
        int durable = staged.prefix();
        int resumeAt = (int) checkpoint.resumeAgainst(new StagedBlocks(staged), true);

        // Before the work, not after it. The re-read below can take minutes on a slow link, and a run
        // that says nothing until it finishes looks indistinguishable from one that is stuck.
        announce(ctx, settings, blockSize, sourceSize, claimed, staged, resumeAt, ids);

        CommitBlockList commit = ctx.resources().registerCommitting(NAME + ":commit",
                new CommitBlockList(client, ids, settings));

        int blocks = upload(in, ctx, client, ids, checkpoint, blockSize, resumeAt, settings, staged);
        commit.blockCount(blocks);
    }

    /**
     * The upload loop. One block is read on this thread and staged on another, up to
     * {@code uploadConcurrency} at a time.
     *
     * <h2>Why parallel at all</h2>
     * A block is a single HTTP request over a WAN, and a WAN link is nowhere near saturated by one
     * connection. Sequentially, 1 TB at a measured per-stream rate is many hours; the only lever that
     * moves that number is the number of streams, because the cost is per byte on a connection rather
     * than per request — the same thing the download side measured.
     *
     * <h2>The three rules this has to keep</h2>
     * <ol>
     *   <li><b>A block counts only once Azure has acknowledged it.</b> {@link UploadWatermark#completed}
     *       is called after {@code stage()} returns and never before, so a block that failed cannot
     *       move the position.</li>
     *   <li><b>The position is the highest <i>contiguous</i> block, not the highest completed one.</b>
     *       With blocks 5, 6 and 7 in flight, 6 finishing first must not advance past 5 — a resume
     *       would then skip 5 entirely and commit a blob with a hole in it. The watermark holds at 4
     *       until 5 lands. Blocks already staged above it are not wasted: their ids are derived rather
     *       than random, so the next attempt's {@code listBlocks} walk finds them and
     *       {@link BlockPrefix.Staged#alreadyHas} skips re-sending them.</li>
     *   <li><b>{@code unitDone} is called only from this thread.</b> Workers record completion; this
     *       loop reads the watermark and advances the checkpoint. {@link StageArtifact} is not
     *       documented as thread-safe, and the fsync-then-append it performs is exactly the kind of
     *       thing that must not interleave.</li>
     * </ol>
     *
     * <h2>Concurrency is a pool size, not a second code path</h2>
     * {@code uploadConcurrency=1} takes this same loop with a one-buffer pool, which blocks the reader
     * until the single worker returns and is therefore sequential. There is deliberately no separate
     * sequential branch: the correctness-critical half of this sink is the ordering above, and a second
     * copy of it would be the copy that gets less review.
     *
     * @return the total number of blocks the blob will be committed from
     */
    private int upload(InputStream in, StreamContext ctx, BlockBlobClient client, BlockIds ids,
                       StageArtifact checkpoint, long blockSize, int resumeAt,
                       AzureBlobSettings settings, BlockPrefix.Staged staged)
            throws StreamException, IOException {

        skipAlreadyUploaded(in, ctx, blockSize, resumeAt);

        boolean verify = settings.verifyBlocks();
        int width = settings.uploadConcurrency();
        AzureBlobSettings.checkBufferBudget(blockSize, width, "blockSize", "uploadConcurrency", NAME);

        UploadWatermark watermark = new UploadWatermark(resumeAt);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicInteger inFlight = new AtomicInteger();

        // The pool IS the concurrency bound: the reader cannot start block N+width until block N has
        // been staged and handed its buffer back, so heap held is blockSize x width and no more.
        ArrayBlockingQueue<byte[]> free = new ArrayBlockingQueue<>(width);
        for (int i = 0; i < width; i++) {
            free.add(new byte[(int) blockSize]);
        }
        ExecutorService pool = Executors.newFixedThreadPool(width, r -> {
            Thread t = new Thread(r, NAME + "-stage");
            t.setDaemon(true);
            return t;
        });
        // The scope owns it, so an aborted run tears the threads down rather than leaking them.
        ctx.resources().register(NAME + ":uploads", () -> pool.shutdownNow());

        int index = resumeAt;
        int checkpointed = resumeAt;
        int reused = 0;
        long bytesThisRun = 0L;
        try {
            while (true) {
                byte[] buffer = takeBuffer(free, failure, ctx, inFlight.get());
                int filled = readFully(in, buffer, ctx);
                if (filled == 0) {
                    free.add(buffer);
                    break;
                }
                BlockSizing.checkIndex(index, blockSize, (long) index * blockSize + filled);

                if (staged.alreadyHas(index)) {
                    // Already on the service from an earlier attempt, so there is nothing to send.
                    // This is not a hole in the ordering rule: the work was done by that attempt and
                    // confirmed by the listBlocks above, which is the same evidence a 201 gives. The
                    // bytes still had to be read, because a stream is sequential and the one resume
                    // offset was spent reaching the prefix — but reading from disk is cheap and
                    // re-sending over a WAN is not.
                    free.add(buffer);
                    watermark.completed(index);
                    reused++;
                } else {
                    final int at = index;
                    final int length = filled;
                    inFlight.incrementAndGet();
                    pool.execute(() -> {
                        try {
                            // 1. do the work. Nothing below runs until Azure has returned 201 for it.
                            stage(client, ids.idFor(at), buffer, length, verify);
                            watermark.completed(at);
                        } catch (Throwable staging) {
                            // First failure wins; later ones are consequences of the unwind.
                            failure.compareAndSet(null, staging);
                        } finally {
                            free.add(buffer);
                            inFlight.decrementAndGet();
                        }
                    });
                }

                index++;
                bytesThisRun += filled;
                // 2 and 3, on this thread only, and never past a gap.
                checkpointed = advance(checkpoint, watermark, checkpointed, blockSize, ctx);
                ctx.job().progress(ctx.stageName(), (long) resumeAt * blockSize + bytesThisRun, index);
                raise(failure);
                if (ctx.job().isCancelled()) {
                    throw new StreamException(NAME + " was asked to stop after " + index + " blocks ("
                            + bytesThisRun + " bytes this attempt); the blob has not been committed",
                            true);
                }
                if (filled < blockSize) {
                    break;
                }
            }

            // Every block must have landed before the block list can name it: the commit happens at
            // scope close, and Put Block List fails outright on an id the service does not hold.
            drain(inFlight, failure, ctx, index);
            checkpointed = advance(checkpoint, watermark, checkpointed, blockSize, ctx);

            if (reused > 0) {
                log.info(NAME + " '" + ctx.stageName() + "': " + reused + " block(s) above the resume"
                        + " point were already on the service and were not sent again ("
                        + BlockSizing.describe((long) reused * blockSize) + " of upload saved). Their"
                        + " bytes were read and discarded, which a sequential stream requires.");
            }

            if (watermark.watermark() != index) {
                // Unreachable unless a worker returned without either staging or failing. Worth saying
                // out loud rather than committing: a gap here is a blob that is the right length and
                // wrong in the middle, which no length check downstream would catch.
                throw new StreamException(NAME + " finished with " + index + " blocks read but the"
                        + " checkpoint at block " + watermark.watermark() + " and "
                        + watermark.strandedAbove() + " stranded above it. Refusing to commit a blob"
                        + " with a hole in it.", false);
            }
        } finally {
            pool.shutdown();
        }
        return index;
    }

    /**
     * Advances the checkpoint to the current watermark, and only when it actually moved.
     *
     * <p>Called once per submitted block, which is more often than the watermark changes: with four
     * in flight it is normal for three calls in a row to find it unmoved. The framework would ignore a
     * repeat anyway, but reporting a position that has not changed makes the log lie about progress.
     */
    private int advance(StageArtifact checkpoint, UploadWatermark watermark, int checkpointed,
                        long blockSize, StreamContext ctx) throws IOException {
        int now = watermark.watermark();
        if (now <= checkpointed) {
            return checkpointed;
        }
        checkpoint.unitDone(now, Long.toString(blockSize));
        // The contiguous watermark, not the highest completed block -- the same number the checkpoint
        // just took, so what a watcher sees and what a resume would use cannot disagree (ADR-0069).
        ctx.status("staged " + now + " blocks of " + blockSize + " bytes");
        // Rate-limited proof that the position is actually moving. Without this the only evidence
        // a checkpoint exists is a row in MFT_CHECKPOINT, which is awkward to watch during a run
        // and gone once it is cleared — so a transfer that checkpoints correctly and one that does
        // not look identical in the log until something kills it.
        if (now == 1 || now % CHECKPOINT_LOG_EVERY == 0) {
            log.info(NAME + " '" + ctx.stageName() + "': checkpoint advanced to block " + now
                    + " (" + BlockSizing.describe((long) now * blockSize)
                    + " durable). A restart from here re-uploads nothing below it.");
        }
        return now;
    }

    /**
     * Waits for a free buffer, which is also how this loop is throttled to {@code uploadConcurrency}.
     *
     * <p>Polled rather than blocked outright. Every worker could have failed while this thread waits,
     * and a plain {@code take()} would then wait for a buffer that is never coming back — a hung
     * transfer where the honest answer is already known.
     */
    private byte[] takeBuffer(ArrayBlockingQueue<byte[]> free, AtomicReference<Throwable> failure,
                              StreamContext ctx, int inFlight) throws StreamException, IOException {
        while (true) {
            raise(failure);
            if (ctx.job().isCancelled()) {
                throw new StreamException(NAME + " was asked to stop while waiting for one of "
                        + inFlight + " in-flight block upload(s); the blob has not been committed",
                        true);
            }
            try {
                byte[] buffer = free.poll(BUFFER_POLL_MILLIS, TimeUnit.MILLISECONDS);
                if (buffer != null) {
                    return buffer;
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IOException(NAME + " was interrupted waiting for a block buffer", interrupted);
            }
        }
    }

    /** Waits for every in-flight block, staying able to notice a failure or a stop while it does. */
    private void drain(AtomicInteger inFlight, AtomicReference<Throwable> failure, StreamContext ctx,
                       int blocks) throws StreamException, IOException {
        while (inFlight.get() > 0) {
            raise(failure);
            if (ctx.job().isCancelled()) {
                throw new StreamException(NAME + " was asked to stop with " + inFlight.get()
                        + " of " + blocks + " block upload(s) still in flight; the blob has not been"
                        + " committed", true);
            }
            try {
                Thread.sleep(BUFFER_POLL_MILLIS);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IOException(NAME + " was interrupted waiting for in-flight blocks",
                        interrupted);
            }
        }
        raise(failure);
    }

    /** Rethrows a worker's failure on the thread that owns the run, keeping its original type. */
    private void raise(AtomicReference<Throwable> failure) throws StreamException, IOException {
        Throwable t = failure.get();
        if (t == null) {
            return;
        }
        if (t instanceof StreamException streamException) {
            throw streamException;
        }
        if (t instanceof IOException io) {
            throw io;
        }
        if (t instanceof RuntimeException runtime) {
            throw runtime;
        }
        if (t instanceof Error error) {
            throw error;
        }
        throw new IOException(NAME + " failed staging a block: " + t.getMessage(), t);
    }

    /**
     * Stages one block, with the integrity check Azure actually honours.
     *
     * <p>{@code Content-MD5} on {@code Put Block} is compared by the service against what arrived and
     * the request fails with 400 on a mismatch, so a corrupted block becomes a retryable error rather
     * than a silently wrong blob. It is deliberately <b>per block</b>: the blob-level
     * {@code x-ms-blob-content-md5} is documented as "not validated, because the hashes for the
     * individual blocks were validated when each was uploaded", so setting that instead would look
     * like a checksum and check nothing.
     *
     * <p>Not computed over the whole blob for a second reason: that needs every byte in order, and a
     * resumed run never reads the prefix it already sent. A per-block hash resumes cleanly because
     * each block is hashed by whichever attempt actually uploads it.
     */
    private static void stage(BlockBlobClient client, String blockId, byte[] buffer, int length,
                              boolean verify) throws IOException {
        InputStream body = new ByteArrayInputStream(buffer, 0, length);
        if (!verify) {
            client.stageBlock(blockId, body, length);
            return;
        }
        client.stageBlockWithResponse(blockId, body, length, md5(buffer, length), null, null,
                Context.NONE);
    }

    /** MD5 over the bytes actually being sent. Azure wants the raw digest; the SDK base64-encodes it. */
    private static byte[] md5(byte[] buffer, int length) throws IOException {
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            digest.update(buffer, 0, length);
            return digest.digest();
        } catch (NoSuchAlgorithmException impossible) {
            throw new IOException("MD5 is required of every JVM", impossible);
        }
    }

    /**
     * Discards the prefix a previous attempt already uploaded.
     *
     * <p><b>Temporary.</b> The source restarts at byte zero because nothing yet hands it a resume
     * offset, so the bytes below the resume point are re-read and thrown away. That still saves the
     * upload, which over a WAN is much the larger half, but it re-reads the source. When the framework
     * can tell a seekable source where to start, this method goes away.
     */
    private void skipAlreadyUploaded(InputStream in, StreamContext ctx, long blockSize, int resumeAt)
            throws IOException {
        if (resumeAt <= 0) {
            return;
        }
        // Only the part the source did not already skip. Both ends read the same number, so when the
        // framework told a seekable source to start at our resume point this comes out zero and no
        // byte is transferred twice. When it could not — a source that cannot seek, a chain with a
        // transform in it — this is the whole prefix, which is the old behaviour and still correct.
        long target = (long) resumeAt * blockSize - ctx.upstreamStartsAt();
        if (target <= 0) {
            log.info(NAME + " resuming at block " + resumeAt + "; the source was started at byte "
                    + ctx.upstreamStartsAt() + ", so nothing is re-read.");
            return;
        }
        long skipped = 0L;
        byte[] sink = new byte[64 * 1024];
        while (skipped < target) {
            long remaining = target - skipped;
            int want = (int) Math.min(sink.length, remaining);
            int read = in.read(sink, 0, want);
            if (read < 0) {
                throw new IOException(NAME + " resumed at block " + resumeAt + " but the source ended"
                        + " after " + skipped + " bytes, short of the " + target + " already uploaded."
                        + " The source is not the one the previous attempt read.");
            }
            skipped += read;
        }
        log.info(NAME + " re-read and discarded " + skipped + " bytes; now uploading from block "
                + resumeAt + ".");
    }

    /**
     * The staged blocks, as the framework's reconciliation sees them.
     *
     * <p>Wraps an answer already in hand rather than asking again: {@code consume} needs the full set
     * for the sparse skip and the prefix for the resume point, and both come from one
     * {@code listBlocks}. A destination that fetched for itself would make that two network calls per
     * resumed run to answer one question twice.
     *
     * <p>Which number wins, and whether anything is discarded, is
     * {@link StageArtifact#resumeAgainst}'s to decide. This sink used to decide it in a private
     * {@code resumePoint}, and the file sink decided the same thing in a private {@code resumeAt}.
     */
    private record StagedBlocks(BlockPrefix.Staged staged) implements StageDestination {

        @Override
        public long durablePosition() {
            // The contiguous prefix, not the highest block present. A block above a gap is on the
            // service but proves nothing about the ones below it, and resuming past the gap would
            // commit a blob that is the right length and wrong in the middle.
            return staged.prefix();
        }

        @Override
        public void discardAbove(long position) {
            BlockPrefix.discardAbove((int) position);
        }
    }

    /**
     * The block size for this attempt. A resumed attempt uses the size <b>recorded by the earlier
     * one</b>: the position counts blocks, so a different size would re-point every recorded block at
     * a different byte range.
     */
    private long blockSize(StreamContext ctx, AzureBlobSettings settings, StageArtifact checkpoint,
                           long sourceSize) throws StreamException, IOException {
        String recorded = checkpoint.resumePayload();
        long configured;
        try {
            configured = settings.blockSizeFor(sourceSize);
        } catch (IllegalArgumentException tooSmall) {
            throw new StreamException(tooSmall.getMessage(), tooSmall, false);
        }
        if (recorded == null) {
            return configured;
        }
        long previous = Long.parseLong(recorded.trim());
        if (previous != configured) {
            log.warn(NAME + " is configured for a block size of " + configured + " bytes but the"
                    + " attempt being resumed used " + previous + "; keeping " + previous + ", because"
                    + " the recorded position counts blocks of that size. Restart the transfer from"
                    + " the beginning to adopt the new size.");
        }
        return previous;
    }

    /**
     * One line, before any work, saying what this attempt is and what it will do.
     *
     * <p>It is emitted on <b>every</b> attempt, not only fresh ones. An earlier version announced only
     * when there was no checkpoint, which made a resumed run quieter than a first run — exactly
     * backwards, since a resume is the more interesting event and the one an operator is looking for.
     *
     * <p>It is also emitted <b>before</b> the re-read rather than after it. On a slow link that skip
     * takes minutes, and a run that says nothing until it completes is indistinguishable from one that
     * has hung.
     *
     * <p>A resumed attempt reports both numbers it reconciled — what the checkpoint claimed and what
     * the service actually holds — because which one won, and why, is the whole of the resume
     * argument. They normally differ: the checkpoint is written after the block it describes, so the
     * service is usually ahead.
     */
    private void announce(StreamContext ctx, AzureBlobSettings settings, long blockSize,
                          long sourceSize, int claimed, BlockPrefix.Staged staged, int resumeAt,
                          BlockIds ids) {
        String stage = ctx.stageName();
        int durable = staged.prefix();
        String ceiling = BlockSizing.describe(BlockSizing.maxBlobFor(blockSize));
        // blockSize x uploadConcurrency, because the buffer pool is sized to the concurrency and every
        // buffer is allocated up front. Reporting one buffer would understate the real figure by
        // whatever that parameter is set to, which is the number a deployer sizing a JVM needs.
        int width = settings.uploadConcurrency();
        String heap = BlockSizing.describe(blockSize * width)
                + (width > 1
                   ? " (" + BlockSizing.describe(blockSize) + " x uploadConcurrency=" + width + ")"
                   : " (uploadConcurrency=1, so the upload is sequential)");
        String blocks = sourceSize > 0
                ? BlockSizing.blocksFor(sourceSize, blockSize) + " blocks expected"
                : "source size unknown";

        if (resumeAt == 0 && claimed == 0) {
            log.info(NAME + " '" + stage + "': starting fresh. blockSize "
                    + BlockSizing.describe(blockSize)
                    + (settings.configuredBlockSize() == null ? " (derived)" : " (configured)")
                    + ", " + blocks + ", ceiling " + ceiling + ", up to " + heap + " of heap, block ids"
                    + " tagged " + ids.tag() + ", integrity "
                    + (settings.verifyBlocks() ? "on (per-block MD5)" : "OFF")
                    + ". The blob will not exist until every block is committed.");
            return;
        }

        int stranded = staged.strandedAbove();
        String reconciled = "checkpoint claimed " + claimed + " block(s), service holds " + durable
                + " contiguous"
                // Named because they are the difference between this attempt and the one before it:
                // they are read past rather than re-sent, which is the whole saving.
                + (stranded > 0
                   ? " plus " + stranded + " above the first gap that will not be sent again"
                   : "");
        if (durable < claimed) {
            log.warn(NAME + " '" + stage + "': RESUMING at block " + durable + " — " + reconciled
                    + ". The checkpoint is ahead of the data, which no check inside this process could"
                    + " have caught, so the service wins and the lower number is used.");
        } else {
            log.info(NAME + " '" + stage + "': RESUMING at block " + resumeAt + " — " + reconciled
                    + (durable > claimed
                       ? " (blocks above the checkpoint landed but were never recorded; they are"
                         + " re-sent under the same ids, which Azure treats as overwrites)"
                       : "")
                    + ". blockSize " + BlockSizing.describe(blockSize) + " (recorded by the earlier"
                    + " attempt), " + blocks + ", block ids tagged " + ids.tag() + ".");
        }
        if (resumeAt > 0 && ctx.upstreamStartsAt() >= (long) resumeAt * blockSize) {
            log.info(NAME + " '" + stage + "': the source was started at byte "
                    + ctx.upstreamStartsAt() + ", so the prefix is skipped rather than re-read —"
                    + " nothing below that offset is transferred at all.");
        } else if (resumeAt > 0) {
            log.info(NAME + " '" + stage + "': about to re-read and discard "
                    + BlockSizing.describe((long) resumeAt * blockSize) + " before uploading anything,"
                    + " because nothing yet hands the source a resume offset. That costs time, not"
                    + " correctness, and it is the whole of what the source-offset work would save.");
        }
    }

    /** Reads until the buffer is full or the stream ends. Returns how many bytes landed. */
    private static int readFully(InputStream in, byte[] buffer, StreamContext ctx)
            throws IOException, StreamException {
        int filled = 0;
        while (filled < buffer.length) {
            int read = in.read(buffer, filled, buffer.length - filled);
            if (read < 0) {
                break;
            }
            filled += read;
            // Checked here and not only once a whole block is staged. A block is 32 MiB by default, so
            // the caller's check cannot fire until a full block has been pulled through the chain and
            // uploaded -- minutes on a slow link, and the whole graceful-shutdown budget on a transfer
            // that has not finished its first block. Stopping mid-block costs nothing: the buffer was
            // never staged, so there is no uncommitted block to reconcile and the position still names
            // the last block Azure acknowledged.
            if (ctx.job().isCancelled()) {
                throw new StreamException(NAME + " was asked to stop while filling a block; the "
                        + filled + " bytes read into it are discarded and the next attempt re-reads"
                        + " them from the last acknowledged block", true);
            }
        }
        return filled;
    }

    /**
     * Captures template parameters while they are bound.
     *
     * <p>Read through {@code lookupTemplateParamater}, not {@code getProperty}. The sequence does not
     * mediate this operator's template — it takes the sink out of the class mediator and drives it
     * directly — so the template's {@code <sequence>} never runs and a {@code <property>} copy of
     * these values would silently never be set. What does happen is
     * {@code InvokeMediator.bindParameters}, which pushes a function context around the call; that is
     * the binding these lookups read, and it lasts only for the duration of the call.
     *
     * <p>The connection is the exception and arrives as a property: {@code configKey} is an attribute
     * on the sequence stage, and {@code bindParameters} mediates that local entry as an ordinary
     * template, so the {@code init} template's own {@code <property name="name"/>} does execute. That
     * is what {@link AzureUtil#getConnectionName} then finds.
     */
    private AzureBlobSettings settings(StreamContext ctx) throws StreamException {
        MessageContext message = ctx.message();
        if (message == null) {
            throw new StreamException(NAME + " has no message context to read its parameters from", false);
        }
        Map<String, String> values = new HashMap<>();
        for (String parameter : PARAMETERS) {
            Object value = ConnectorUtils.lookupTemplateParamater(message, parameter);
            if (value != null) {
                values.put(parameter, value.toString());
            }
        }
        try {
            return AzureBlobSettings.from(values, NAME);
        } catch (IllegalArgumentException invalid) {
            throw new StreamException(invalid.getMessage(), invalid, false);
        }
    }

    private BlockBlobClient blockBlobClient(StreamContext ctx, AzureBlobSettings settings)
            throws StreamException {
        try {
            String connectionName = AzureUtil.getConnectionName(ctx.message());
            AzureStorageConnectionHandler connection = (AzureStorageConnectionHandler)
                    ConnectionHandler.getConnectionHandler()
                            .getConnection(AzureConstants.CONNECTOR_NAME, connectionName);
            BlobServiceClient service = connection.getBlobServiceClient();
            BlobContainerClient container = service.getBlobContainerClient(settings.container());
            return container.getBlobClient(settings.blob()).getBlockBlobClient();
        } catch (Exception unreachable) {
            // Retryable: a queued transfer may well find the account reachable on the next attempt.
            throw new StreamException(NAME + " could not reach container '" + settings.container()
                    + "': " + unreachable.getMessage(), unreachable, true);
        }
    }
}
