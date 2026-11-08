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

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.core.util.Context;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.models.DownloadRetryOptions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.MessageContext;
import org.apache.synapse.stream.SourceIdentity;
import org.apache.synapse.stream.StreamContext;
import org.apache.synapse.stream.StreamException;
import org.apache.synapse.stream.StreamOrigin;
import org.apache.synapse.stream.StreamSource;
import org.wso2.carbon.connector.azure.storage.connection.AzureStorageConnectionHandler;
import org.wso2.carbon.connector.azure.storage.util.AzureConstants;
import org.wso2.carbon.connector.azure.storage.util.AzureUtil;
import org.wso2.carbon.connector.core.AbstractConnector;
import org.wso2.carbon.connector.core.ConnectException;
import org.wso2.carbon.connector.core.connection.ConnectionHandler;
import org.wso2.carbon.connector.core.util.ConnectorUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Reads an Azure blob as the head of a stream sequence.
 *
 * <pre>
 *   &lt;azure.blobStreamRead configKey="connBlob"&gt;
 *     &lt;container&gt;inbox&lt;/container&gt;
 *     &lt;blob&gt;reports/in.csv&lt;/blob&gt;
 *   &lt;/azure.blobStreamRead&gt;
 * </pre>
 *
 * <h2>Much smaller than the sink, and that is the point</h2>
 * A blob is one stream. There is no chunking here, no block accounting and <b>no checkpoint</b> —
 * {@code checkpointed()} stays false, and a source structurally cannot hold a position anyway: a stage
 * that keeps one without materialising must be the last stage, and a source never is. All the resume
 * machinery lives on the sink. This one has two jobs: open the right bytes, and prove they did not
 * change.
 *
 * <p>That asymmetry is also why this class may hand the work to the SDK where the sink may not.
 * {@code BlobInputStream} already reads in ranges and retries a dropped connection internally, and
 * nothing here has to survive a crash, so there is no reason to reimplement it. The sink cannot use
 * the equivalent {@code BlobOutputStream} because it would have to give up the block ids and the
 * per-block acknowledgement that resume is built on — see {@link AzureBlobStreamWrite}.
 *
 * <h2>Determinism, and what it actually rests on</h2>
 * {@code deterministic()} is true, but not because reading a blob is inherently repeatable — it is
 * true <i>because</i> {@link #identify} gives the framework a strong change token and the guard
 * refuses a resumed run whose source moved underneath it. The ETag is what makes the claim honest.
 *
 * <p>Stateless: one instance serves every concurrent run, so the settings live in locals and in the
 * returned stream, never in a field.
 */
public class AzureBlobStreamRead extends AbstractConnector implements StreamSource {

    private static final String NAME = "azure.blobStreamRead";

    @Override
    public void connect(MessageContext messageContext) throws ConnectException {
        throw new ConnectException(NAME + " is only valid inside a <streamSequence>. It opens a byte"
                + " chain rather than mediating a message, so there is nothing for it to do in a"
                + " sequence. Use msazurestorage.downloadBlob instead.");
    }

    @Override
    public String name() {
        return NAME;
    }

    /**
     * True only because the identity guard backs it: {@link #identify} returns the blob's ETag, and a
     * resumed run whose blob changed is refused rather than resumed onto different bytes. Without that
     * this would be a claim nobody could keep.
     */
    @Override
    public boolean deterministic() {
        return true;
    }

    /**
     * Yes — and it costs nothing, because every read here is already a ranged GET.
     *
     * <p>{@code downloadStreamWithResponse(sink, new BlobRange(at, want), ...)} is what this source
     * does for every range anyway, so beginning at byte 450,000,000 instead of 0 is a different value
     * of {@code at}. The bytes below it are never requested, never transferred and never paid for.
     */
    @Override
    public boolean seekable() {
        return true;
    }

    /** A blob can be opened again from the start, which is what lets a failed run retry at all. */
    @Override
    public StreamOrigin origin() {
        return StreamOrigin.REOPENABLE;
    }

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
     * Performs <b>no I/O</b> — invariant 1. Everything is deferred into the returned stream, which
     * acquires the client and issues the first request on the first {@code read()}. Building a chain
     * must not touch the network, or a sequence that is never read still costs a round trip, and a
     * failure during the build lands somewhere with no stage to blame.
     *
     * <p>The connection <b>name</b> is captured here, not when the stream first reads. It arrives as
     * a {@code name} property that this stage's binding sets on the shared message, and the binding
     * lasts only for this call: the sink binds next and sets the same property to its own
     * connection. Read lazily, a source in front of a sink on another connector looks up the sink's
     * connection name under the Azure connector and fails with "Connection ... for azurestorage
     * connector does not exist". Looking a name up is not I/O, so invariant 1 still holds.
     */
    @Override
    public InputStream open(StreamContext ctx) throws StreamException {
        AzureBlobSettings settings = settings(ctx);
        return new RangedBlobStream(ctx, settings, connectionName(ctx));
    }

    /**
     * One {@code getProperties()} — the one place a source is expected to do I/O.
     *
     * <p>The identity is <b>container, blob and ETag</b>. ETag is a strong change token: it moves on
     * every write, including one that leaves the size and timestamp untouched, which is exactly the
     * case a size-and-mtime identity misses. ADR-0033 exists for this — an identity that collides
     * across different content makes a resumed run splice two blobs together, and too weak is worse
     * than absent.
     *
     * <p>No SAS token, account key or resolved URL goes into that string. It is persisted to a job row
     * that others can read and printed on a guard mismatch.
     *
     * <p>Failures are retryable: a queued transfer may find the blob present, or the account
     * reachable, on the next attempt. Returning {@code null} is legitimate and means "I cannot say
     * cheaply", which costs a re-run rather than correctness.
     */
    @Override
    public SourceIdentity identify(StreamContext ctx) throws StreamException {
        if (ctx.message() == null) {
            return null;
        }
        AzureBlobSettings settings;
        try {
            settings = settings(ctx);
        } catch (StreamException notConfiguredYet) {
            return null;
        }
        try {
            BlobProperties properties = blobClient(connectionName(ctx), settings).getProperties();
            String etag = properties.getETag();
            if (etag == null || etag.isBlank()) {
                return null;
            }
            long lastModified = properties.getLastModified() == null
                    ? 0L : properties.getLastModified().toInstant().toEpochMilli();
            return new SourceIdentity(
                    settings.container() + "|" + settings.blob() + "|" + etag,
                    properties.getBlobSize(), lastModified);
        } catch (RuntimeException notReadable) {
            throw new StreamException(NAME + " could not read '" + settings.container() + "/"
                    + settings.blob() + "': " + notReadable.getMessage(), notReadable, true);
        }
    }

    private AzureBlobSettings settings(StreamContext ctx) throws StreamException {
        MessageContext message = ctx.message();
        if (message == null) {
            throw new StreamException(NAME + " has no message context to read its parameters from", false);
        }
        Map<String, String> values = new HashMap<>();
        for (String parameter : AzureBlobSettings.PARAMETERS) {
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

    /**
     * The connection this stage was configured with. Valid only while the stage is bound, which is
     * during {@code open()} and {@code identify()}; see {@link #open}.
     */
    private static String connectionName(StreamContext ctx) throws StreamException {
        try {
            return AzureUtil.getConnectionName(ctx.message());
        } catch (Exception unresolved) {
            throw new StreamException(NAME + " has no connection: " + unresolved.getMessage(),
                    unresolved, false);
        }
    }

    private static BlobClient blobClient(String connectionName, AzureBlobSettings settings)
            throws StreamException {
        try {
            AzureStorageConnectionHandler connection = (AzureStorageConnectionHandler)
                    ConnectionHandler.getConnectionHandler()
                            .getConnection(AzureConstants.CONNECTOR_NAME, connectionName);
            BlobServiceClient service = connection.getBlobServiceClient();
            BlobContainerClient container = service.getBlobContainerClient(settings.container());
            return container.getBlobClient(settings.blob());
        } catch (Exception unreachable) {
            throw new StreamException(NAME + " could not reach container '" + settings.container()
                    + "': " + unreachable.getMessage(), unreachable, true);
        }
    }

    /**
     * Reads the blob in explicit ranges, rather than through {@code BlobInputStream}.
     *
     * <h2>Why not the SDK's stream, after all</h2>
     * An earlier version of this class used {@code openInputStream}, on the reasoning that the SDK
     * already does ranged reads and retries internally so the source could stay thin. Measurement
     * against a real account said otherwise: the same 512 MiB blob uploaded at about 4.3 MiB/s and
     * downloaded at <b>68 KiB/s</b> — one 4 MiB dispatch per 60 seconds, which is the dispatch size
     * and the default response timeout, not a transfer rate. A thread dump showed the transfer parked
     * in {@code Mono.block()} inside {@code BlobInputStream.dispatchRead}, having used 537 ms of CPU
     * in 396 seconds, with every reactor event loop idle. It was waiting, not working.
     *
     * <p>Three things follow, and only the first is about speed:
     * <ol>
     *   <li>{@code BlobInputStream} exposes <b>no timeout we control</b>, so a stalled response is
     *       indistinguishable from a slow one and the transfer cannot fail fast.</li>
     *   <li>{@code Mono.block()} is <b>uninterruptible</b>. A run parked there cannot be stopped by
     *       cancellation or graceful shutdown, because nothing can interrupt a single {@code read()} —
     *       the sink polls between blocks and never gets the chance.</li>
     *   <li>Its retries are invisible: there is no way to see one happen or to bound them.</li>
     * </ol>
     *
     * <p>{@code downloadStreamWithResponse} takes an explicit {@link Duration} timeout and explicit
     * {@link DownloadRetryOptions}, which fixes all three. A range that will not move becomes a
     * retryable failure the framework can act on instead of a thread parked for ever.
     *
     * <p>The timeout is a <b>liveness bound, not a performance target</b>. A range that cannot move
     * a full range within its scaled bound means the connection is dead; failing the run so it
     * can be retried is strictly better than waiting.
     */
    private static final class RangedBlobStream extends InputStream {

        private static final Log rangeLog = LogFactory.getLog(RangedBlobStream.class);

        /**
         * The slowest per-stream rate we are willing to call alive. Measured throughput to a
         * far-region account was about 68 KiB/s per stream, so this is set well below it: the bound
         * exists to catch a dead connection, not to enforce a service level.
         */
        private static final long MIN_BYTES_PER_SECOND = 16 * 1024;

        private static final Duration MIN_RANGE_TIMEOUT = Duration.ofMinutes(2);
        private static final int RANGE_RETRIES = 3;

        /** How often the ordered wait looks up to see whether the run has been told to stop. */
        private static final long CANCEL_POLL_MILLIS = 250L;

        private final StreamContext ctx;
        private final AzureBlobSettings settings;
        /** Captured in {@code open()} while bound; the message property may since name another. */
        private final String connectionName;

        private BlobClient blob;
        private BlobRequestConditions unchanged;
        private long blobSize = -1L;
        private long position;

        private byte[] buffer = new byte[0];
        private int offset;
        private int limit;

        /** Ranges in flight, in the order they must be served. Touched only by the consume thread. */
        private final Deque<Future<byte[]>> inFlight = new ArrayDeque<>();
        private ExecutorService pool;
        private long nextRange;
        private int rangeBytes;
        private boolean verifyRanges;

        RangedBlobStream(StreamContext ctx, AzureBlobSettings settings, String connectionName) {
            this.ctx = ctx;
            this.settings = settings;
            this.connectionName = connectionName;
        }

        /** First touch of the network, deferred out of {@code open()} by invariant 1. */
        private void start() throws IOException {
            if (blob != null) {
                return;
            }
            try {
                BlobClient client = blobClient(connectionName, settings);
                BlobProperties properties = client.getProperties();
                blobSize = properties.getBlobSize();
                // Pinned for every range. A blob rewritten mid-transfer fails the next range with 412
                // rather than handing back a splice of two versions.
                unchanged = new BlobRequestConditions().setIfMatch(properties.getETag());
                ctx.job().sourceOpened(blobSize, properties.getLastModified() == null
                        ? 0L : properties.getLastModified().toInstant().toEpochMilli());
                // A resumed run starts where the sink already got to, rather than re-reading a prefix
                // only to throw it away. Zero on a first attempt, and zero whenever the framework
                // could not establish an offset — both of which are simply "start at the beginning".
                long from = ctx.upstreamStartsAt();
                if (from > 0) {
                    nextRange = from;
                    position = from;
                    rangeLog.info(NAME + " skipping directly to byte " + from + " of "
                            + BlockSizing.describe(blobSize) + "; the prefix is never requested");
                }
                int width = settings.readConcurrency();
                rangeBytes = (int) settings.readBlockSize();
                verifyRanges = settings.verifyRanges();
                pool = Executors.newFixedThreadPool(width, r -> {
                    Thread t = new Thread(r, NAME + "-range");
                    t.setDaemon(true);
                    return t;
                });
                // The scope owns it, so an aborted run tears the threads down rather than leaking them.
                ctx.resources().register(NAME + ":ranges", () -> pool.shutdownNow());
                blob = client;
                rangeLog.info(NAME + " reading '" + settings.container() + "/" + settings.blob()
                        + "' (" + BlockSizing.describe(blobSize) + ") in "
                        + BlockSizing.describe(rangeBytes) + " ranges, " + width + " at a time; "
                        + BlockSizing.blocksFor(blobSize, rangeBytes) + " requests, holding "
                        + BlockSizing.describe((long) rangeBytes * width) + " of heap."
                        + (settings.readBuffersWorthRemarkingOn()
                           ? " That is over a quarter of this JVM's heap; configured deliberately,"
                             + " recorded here so it is visible."
                           : "")
                        + (verifyRanges
                           ? " Each range is checked against a Content-MD5 from the service."
                           : " Range MD5 is OFF: Azure only hashes ranges at or below "
                             + BlockSizing.describe(AzureBlobSettings.MAX_VERIFIABLE_RANGE)
                             + (settings.verifyBlocks()
                                ? ", and readBlockSize is larger. Lower it to restore the check."
                                : " and verifyBlocks is false.")));
            } catch (StreamException notReachable) {
                throw new IOException(notReachable.getMessage(), notReachable);
            } catch (RuntimeException failed) {
                throw new IOException(NAME + " could not open '" + settings.container() + "/"
                        + settings.blob() + "': " + failed.getMessage(), failed);
            }
        }

        /**
         * Serves the next range, keeping {@code readConcurrency} of them in flight.
         *
         * <h2>Why this is not an optimisation</h2>
         * A single TCP stream carries {@code window / RTT}. The storage account measured here is about
         * 285 ms away, so one stream is capped near 0.25-1 MiB/s however fast the link is — and each
         * 4 MiB range pays roughly nine round trips of slow-start, about 2.7 s, before it streams at
         * all. Concurrency is what makes a far-region download usable: N ranges in flight multiply both
         * the effective window and the slow-start amortisation.
         *
         * <p>Requests are issued out of order and <b>served strictly in order</b> — the queue is
         * drained from the front, so nothing downstream ever sees a byte out of sequence. That is why
         * the source needs no watermark and no checkpoint, unlike the sink: out-of-order <i>arrival</i>
         * is invisible, because delivery is what matters.
         *
         * <p>Held memory is {@code readBlockSize x readConcurrency} — 32 MiB at the defaults. Note
         * that raising {@code readBlockSize} is the stronger lever: it cuts the number of requests,
         * where raising concurrency only runs the same expensive requests side by side.
         *
         * @return false at end of blob
         */
        private boolean fill() throws IOException {
            start();
            if (offset < limit) {
                return true;
            }
            while (inFlight.size() < settings.readConcurrency() && nextRange < blobSize) {
                final long at = nextRange;
                final int want = (int) Math.min(rangeBytes, blobSize - at);
                nextRange += want;
                inFlight.add(pool.submit(() -> downloadRange(at, want)));
            }
            if (inFlight.isEmpty()) {
                return false;
            }
            buffer = awaitHead(inFlight.poll());
            offset = 0;
            limit = buffer.length;
            position += limit;
            return true;
        }

        /**
         * Waits for the next range in order, while staying able to notice a stop request.
         *
         * <p><b>Why this is not a plain {@code get()}.</b> Ranges are issued concurrently and share one
         * link, so each one completes at roughly the link rate divided by {@code readConcurrency} --
         * and delivery is strictly ordered, so nothing downstream moves until range 0 lands. On a
         * measured 0.7 MiB/s link with 16 ranges of 4 MiB in flight, that first range takes about 90
         * seconds. An unbounded {@code get()} parks the sink for all of it, and a sink parked inside
         * {@code read()} cannot check whether it has been told to stop. A graceful shutdown therefore
         * waited out the whole range before anything noticed: observed at 96s against a 3s request.
         *
         * <p>Polling a short wait costs nothing and decouples the two: the bytes still take as long as
         * the link takes, but a stop is seen within {@link #CANCEL_POLL_MILLIS} of being asked.
         *
         * <p>The in-flight range is cancelled rather than drained. Discarding a partly-downloaded range
         * costs one re-request on the next attempt, which resumes from the sink's position anyway;
         * waiting for it costs the whole shutdown budget.
         *
         * <p><b>Never returns short or signals EOF on a stop.</b> It throws, because the sink reads
         * {@code -1} as "the source is finished" and would commit a truncated file as a complete one.
         * The sequence classifies the failure by {@code isCancelled()} rather than by exception type,
         * so a throw here is still recorded as a stop and the run stays resumable.
         */
        private byte[] awaitHead(Future<byte[]> head) throws IOException {
            while (true) {
                try {
                    return head.get(CANCEL_POLL_MILLIS, TimeUnit.MILLISECONDS);
                } catch (TimeoutException stillRunning) {
                    if (ctx.job().isCancelled()) {
                        head.cancel(true);
                        throw new IOException(NAME + " stopped while waiting for bytes " + position
                                + " onwards of '" + settings.container() + "/" + settings.blob()
                                + "'; the range was discarded and the next attempt re-requests it");
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    head.cancel(true);
                    throw new IOException(NAME + " was interrupted waiting for a range", interrupted);
                } catch (ExecutionException failed) {
                    Throwable cause = failed.getCause();
                    throw cause instanceof IOException io ? io
                            : new IOException(NAME + " failed reading a range: " + cause.getMessage(),
                                    cause);
                }
            }
        }

        /**
         * How long a range of this size may take before the connection is presumed dead.
         *
         * <p>A configured {@code readTimeout} wins outright. The derived value below is only a
         * fallback, and its floor rate came from measurements in one environment — see
         * {@link AzureBlobSettings#readTimeoutSeconds()} for why that must not be the last word.
         *
         * <p><b>Must scale with the range.</b> A fixed bound was a bug: measured per-stream throughput
         * is roughly constant in bytes per second whatever the range size, so a 32 MiB range needs
         * eight times as long as a 4 MiB one. A flat three minutes passed the small ranges and failed
         * every large one, which made {@code readBlockSize} look broken when it was the timeout.
         */
        private Duration timeoutFor(int bytes) {
            Long configured = settings.readTimeoutSeconds();
            if (configured != null) {
                return Duration.ofSeconds(configured);
            }
            Duration scaled = Duration.ofSeconds(Math.max(1, bytes / MIN_BYTES_PER_SECOND));
            return scaled.compareTo(MIN_RANGE_TIMEOUT) > 0 ? scaled : MIN_RANGE_TIMEOUT;
        }

        /** One ranged GET, bounded in time so a stall fails the run instead of parking it for ever. */
        private byte[] downloadRange(long at, int want) throws IOException {
            ByteArrayOutputStream sink = new ByteArrayOutputStream(want);
            try {
                blob.downloadStreamWithResponse(sink, new BlobRange(at, (long) want),
                        new DownloadRetryOptions().setMaxRetryRequests(RANGE_RETRIES),
                        unchanged,
                        // Azure computes and the SDK verifies a Content-MD5 for the range, offered only
                        // at or below 4 MiB. Symmetric to the per-block MD5 on upload, and the reason
                        // RANGE_BYTES is not larger.
                        verifyRanges,
                        timeoutFor(want), Context.NONE);
            } catch (RuntimeException failed) {
                throw new IOException(NAME + " failed reading bytes " + at + "-" + (at + want - 1)
                        + " of '" + settings.container() + "/" + settings.blob() + "' within "
                        + timeoutFor(want) + ": " + failed.getMessage()
                        + ". This is a liveness bound, not a rate limit; a range this size is allowed "
                        + MIN_BYTES_PER_SECOND / 1024 + " KiB/s before it is called dead.", failed);
            }
            byte[] bytes = sink.toByteArray();
            if (bytes.length == 0) {
                throw new IOException(NAME + " asked for " + want + " bytes at " + at
                        + " and got none; the blob is shorter than its reported size of " + blobSize);
            }
            return bytes;
        }

        @Override
        public int read() throws IOException {
            if (!fill()) {
                return -1;
            }
            return buffer[offset++] & 0xFF;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (len == 0) {
                return 0;
            }
            if (!fill()) {
                return -1;
            }
            int n = Math.min(len, limit - offset);
            System.arraycopy(buffer, offset, b, off, n);
            offset += n;
            return n;
        }

        @Override
        public int available() {
            return limit - offset;
        }

        @Override
        public void close() {
            for (Future<byte[]> pending : inFlight) {
                pending.cancel(true);
            }
            inFlight.clear();
            if (pool != null) {
                pool.shutdownNow();
            }
            buffer = new byte[0];
            offset = 0;
            limit = 0;
        }
    }
}
