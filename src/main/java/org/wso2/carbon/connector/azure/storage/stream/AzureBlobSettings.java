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

import java.util.Locale;
import java.util.Map;

/**
 * Everything the streaming blob operators read from their template, parsed once.
 *
 * <p>The rules live in {@link #from(Map, String)} and nowhere else, so the deployment-time check and
 * the runtime parse cannot drift apart. A second, looser checker is how a configuration passes
 * validation and then fails on a customer's file.
 *
 * <p>An unset template parameter arrives as an <b>empty string</b>, not null, so {@code != null} is
 * never the right test; {@link #value(Map, String)} treats blank as absent, once, for every field.
 *
 * <p>Instances are immutable. Operators must capture one while their parameters are bound and hold it
 * as a local — never a field, because one operator instance serves every concurrent run.
 */
public final class AzureBlobSettings {

    public static final int MIN_CONCURRENCY = 1;
    /**
     * A bound on typos, not on ambition. These threads are I/O bound — each parks on a socket waiting
     * for a ranged GET — so core count is nearly irrelevant and there is no reason to tie the number to
     * it. It is set high enough that no deliberate configuration should meet it; what a deployment
     * actually runs out of is memory, and that is the deployer's call to make, not this connector's.
     */
    public static final int MAX_CONCURRENCY = 256;
    public static final int DEFAULT_CONCURRENCY = 4;

    private static final long MIB = 1024L * 1024L;
    /** Largest range Azure will compute a Content-MD5 for. */
    public static final long MAX_VERIFIABLE_RANGE = 4 * MIB;
    public static final long MIN_READ_BLOCK = MIB;
    public static final long MAX_READ_BLOCK = 64 * MIB;
    /** 8 MiB: half the requests of 4 MiB, 32 MiB held at the default concurrency of 4. */
    public static final long DEFAULT_READ_BLOCK = 8 * MIB;

    /**
     * Share of the heap a single transfer's read buffers may hold.
     *
     * <p>Above this the operator logs what it is holding, and carries on. It does not refuse: how much
     * of its own heap a deployment spends on read buffers is the deployer's decision, the same way
     * {@code maxReprocessed} is validated against nothing and merely recorded. A connector that
     * second-guesses that is overriding someone who knows their server better than it does.
     *
     * <p>Derived from {@code maxMemory()} rather than fixed, because the threshold worth remarking on
     * depends on the deployment: numbers that are reckless on a 512m server are unremarkable on an 8g
     * one.
     */
    public static final double READ_BUFFER_HEAP_SHARE = 0.25;

    private final String container;
    private final String blob;
    private final Long blockSize;
    private final String contentType;
    private final boolean overwrite;
    private final int uploadConcurrency;
    private final int readConcurrency;
    private final boolean verifyBlocks;
    private final long readBlockSize;
    private final Long readTimeoutSeconds;

    private AzureBlobSettings(String container, String blob, Long blockSize, String contentType,
                              boolean overwrite, int uploadConcurrency, int readConcurrency,
                              boolean verifyBlocks, long readBlockSize,
                              Long readTimeoutSeconds) {
        this.container = container;
        this.blob = blob;
        this.blockSize = blockSize;
        this.contentType = contentType;
        this.overwrite = overwrite;
        this.uploadConcurrency = uploadConcurrency;
        this.readConcurrency = readConcurrency;
        this.verifyBlocks = verifyBlocks;
        this.readBlockSize = readBlockSize;
        this.readTimeoutSeconds = readTimeoutSeconds;
    }

    /**
     * The one parse. Reused by {@code validateConfiguration} over literal parameters and by the
     * operator over bound ones.
     *
     * @throws IllegalArgumentException with a message naming the parameter and what was wrong with it
     */
    public static AzureBlobSettings from(Map<String, String> values, String operator) {
        String container = required(values, "container", operator);
        String blob = required(values, "blob", operator);

        Long blockSize = null;
        String rawBlockSize = value(values, "blockSize");
        if (rawBlockSize != null) {
            blockSize = parseSize(rawBlockSize, operator);
            // Checked without a source size: only the absolute range is decidable here. Whether it
            // fits a particular source is checked once the source has been probed.
            BlockSizing.validate(blockSize, 0L);
        }

        long readBlock = readBlockSize(values, operator);
        int readWidth = concurrency(values, "readConcurrency", operator);
        checkBufferBudget(readBlock, readWidth, operator);

        return new AzureBlobSettings(container, blob, blockSize, value(values, "contentType"),
                flag(values, "overwrite", true, operator),
                concurrency(values, "uploadConcurrency", operator),
                readWidth,
                flag(values, "verifyBlocks", true, operator),
                readBlock,
                readTimeout(values, operator));
    }

    public String container() {
        return container;
    }

    public String blob() {
        return blob;
    }

    /** The configured block size, or null to derive one from the source size at run time. */
    public Long configuredBlockSize() {
        return blockSize;
    }

    public String contentType() {
        return contentType;
    }

    public boolean overwrite() {
        return overwrite;
    }

    public int uploadConcurrency() {
        return uploadConcurrency;
    }

    public int readConcurrency() {
        return readConcurrency;
    }

    /**
     * Whether to send each block's MD5 so Azure rejects one that arrived corrupted.
     *
     * <p>On by default. Azure verifies the hash <b>per block, as it lands</b>, and fails the request
     * with 400 on a mismatch — it is not an end-of-upload check. The blob-level
     * {@code x-ms-blob-content-md5} is a different thing and is explicitly <i>not</i> validated by the
     * service, so it buys no integrity and is not set here.
     *
     * <p>Hashing a 32 MiB buffer costs tens of milliseconds against a multi-second upload, so the
     * switch exists for the rare deployment that has measured it and wants the time back, not because
     * the default is in doubt.
     */
    public boolean verifyBlocks() {
        return verifyBlocks;
    }

    /**
     * The block size to actually use: the configured one, or one derived to fit {@code sourceSize}.
     *
     * @param sourceSizeBytes the probed source size, or a non-positive value when it is unknown
     */
    public long blockSizeFor(long sourceSizeBytes) {
        if (blockSize == null) {
            return BlockSizing.derive(sourceSizeBytes);
        }
        BlockSizing.validate(blockSize, sourceSizeBytes);
        return blockSize;
    }

    /** Bytes per ranged GET on the download side. See {@link #readBlockSize()}. */
    private static long readBlockSize(Map<String, String> values, String operator) {
        String raw = value(values, "readBlockSize");
        if (raw == null) {
            return DEFAULT_READ_BLOCK;
        }
        long size = parseSize(raw, operator);
        if (size < MIN_READ_BLOCK || size > MAX_READ_BLOCK) {
            throw new IllegalArgumentException(operator + " [readBlockSize] must be between "
                    + MIN_READ_BLOCK / (1024 * 1024) + " MiB and " + MAX_READ_BLOCK / (1024 * 1024)
                    + " MiB, but was " + size + " bytes. It is held in heap readConcurrency times over.");
        }
        return size;
    }

    /**
     * Bytes fetched per ranged GET.
     *
     * <p><b>This is the high-leverage setting on a far-region download</b>, more so than
     * {@link #readConcurrency()}. Throughput to a distant account is dominated by per-request cost —
     * round-trip latency and TCP slow-start, paid again on every range — so halving the number of
     * requests helps more than doubling the number of threads, and costs no extra threads. The upload
     * side is fast for exactly this reason: 32 MiB blocks against 4 MiB ranges is 8x fewer requests
     * over the same link.
     *
     * <p><b>The trade:</b> Azure computes a per-range Content-MD5 only at or below 4 MiB. Above that,
     * range verification is not available and is switched off with a log line saying so. Integrity on
     * the way down then rests on TLS and on whatever the consumer checks, rather than on a hash the
     * service compared.
     *
     * <p>Held memory is {@code readBlockSize x readConcurrency} per transfer.
     */
    public long readBlockSize() {
        return readBlockSize;
    }

    /** True only when the range is small enough that Azure will hash it. */
    public boolean verifyRanges() {
        return verifyBlocks && readBlockSize <= MAX_VERIFIABLE_RANGE;
    }

    /**
     * Refuses only what cannot work.
     *
     * <p>{@code readBlockSize x readConcurrency} is held in heap for the whole transfer. Asking for
     * more than the entire heap is not an aggressive choice, it is arithmetic that fails every time,
     * and it fails as an OutOfMemoryError with nothing pointing at the configuration that caused it —
     * so it is worth catching at deployment, where the message can name both factors.
     *
     * <p>Everything short of that is left alone and logged instead. A deployer running 48 readers of
     * 32 MiB on an 8g server knows something this connector does not.
     */
    static void checkBufferBudget(long readBlockSize, int readConcurrency, String operator) {
        checkBufferBudget(readBlockSize, readConcurrency, "readBlockSize", "readConcurrency", operator);
    }

    /**
     * As above, for either side. The upload side cannot always be checked at deployment — its block
     * size is derived from the probed source size when none is configured — so the sink checks again
     * once it knows, immediately before allocating the pool.
     */
    static void checkBufferBudget(long bufferSize, int concurrency, String sizeParam,
                                  String widthParam, String operator) {
        long held = bufferSize * concurrency;
        long heap = Runtime.getRuntime().maxMemory();
        if (held >= heap) {
            throw new IllegalArgumentException(operator + ": " + sizeParam + " " + bufferSize / MIB
                    + " MiB x " + widthParam + " " + concurrency + " would hold " + held / MIB
                    + " MiB of buffers, and this JVM's entire max heap is " + heap / MIB
                    + " MiB. That cannot run. Lower either, or give the server more heap — concurrency"
                    + " is the cheaper one to keep, since these threads wait on the network rather"
                    + " than on a core.");
        }
    }

    /** True when the buffers are large enough that the operator should say so in the log. */
    public boolean readBuffersWorthRemarkingOn() {
        return readBlockSize * readConcurrency
                > (long) (Runtime.getRuntime().maxMemory() * READ_BUFFER_HEAP_SHARE);
    }

    private static Long readTimeout(Map<String, String> values, String operator) {
        String raw = value(values, "readTimeout");
        if (raw == null) {
            return null;
        }
        String text = raw.trim().toLowerCase(Locale.ROOT);
        long multiplier = text.endsWith("m") ? 60L : 1L;
        String digits = text.endsWith("m") || text.endsWith("s")
                ? text.substring(0, text.length() - 1) : text;
        long seconds;
        try {
            seconds = Long.parseLong(digits.trim()) * multiplier;
        } catch (NumberFormatException notANumber) {
            throw new IllegalArgumentException(operator + " [readTimeout] must be seconds, optionally"
                    + " suffixed s or m, but was '" + raw + "'");
        }
        if (seconds < 1) {
            throw new IllegalArgumentException(
                    operator + " [readTimeout] must be at least one second, but was '" + raw + "'");
        }
        return seconds;
    }

    /**
     * Per-range liveness bound, in seconds, or null to derive one.
     *
     * <p><b>Configurable on purpose.</b> The derived value scales with the range size against a floor
     * rate, and that floor was chosen against throughput measured in one environment. Link speed and
     * distance to the storage account vary enormously between deployments, so a constant picked in a
     * lab is the wrong thing to freeze into a connector: too generous and a dead connection hangs for
     * minutes, too tight and a slow but healthy link fails every range. Set it where the deployment is
     * known; leave it unset and the derived bound applies.
     */
    public Long readTimeoutSeconds() {
        return readTimeoutSeconds;
    }

    private static String value(Map<String, String> values, String name) {
        String raw = values == null ? null : values.get(name);
        if (raw == null) {
            return null;
        }
        String trimmed = raw.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private static String required(Map<String, String> values, String name, String operator) {
        String found = value(values, name);
        if (found == null) {
            throw new IllegalArgumentException(operator + " requires a non-empty [" + name + "]");
        }
        return found;
    }

    private static boolean flag(Map<String, String> values, String name, boolean fallback,
                                String operator) {
        String raw = value(values, name);
        if (raw == null) {
            return fallback;
        }
        if ("true".equalsIgnoreCase(raw)) {
            return true;
        }
        if ("false".equalsIgnoreCase(raw)) {
            return false;
        }
        throw new IllegalArgumentException(
                operator + " [" + name + "] must be true or false, but was '" + raw + "'");
    }

    private static int concurrency(Map<String, String> values, String name, String operator) {
        String raw = value(values, name);
        if (raw == null) {
            return DEFAULT_CONCURRENCY;
        }
        int parsed;
        try {
            parsed = Integer.parseInt(raw);
        } catch (NumberFormatException notANumber) {
            throw new IllegalArgumentException(
                    operator + " [" + name + "] must be a whole number, but was '" + raw + "'");
        }
        if (parsed < MIN_CONCURRENCY || parsed > MAX_CONCURRENCY) {
            throw new IllegalArgumentException(operator + " [" + name + "] must be between "
                    + MIN_CONCURRENCY + " and " + MAX_CONCURRENCY + ", but was " + parsed
                    + ". Each concurrent transfer holds blockSize x this many bytes of heap.");
        }
        return parsed;
    }

    /**
     * Accepts a plain byte count or a binary-suffixed one — {@code 33554432}, {@code 32M},
     * {@code 32MB} and {@code 32MiB} are the same size. Suffixes are binary because Azure's own
     * limits are stated that way, and a deployer writing 4000MB against a 4000 MiB ceiling should not
     * be told it is too large.
     */
    private static long parseSize(String raw, String operator) {
        String text = raw.trim().toUpperCase(Locale.ROOT).replace("IB", "B");
        long multiplier = 1L;
        if (text.endsWith("KB") || text.endsWith("K")) {
            multiplier = 1024L;
        } else if (text.endsWith("MB") || text.endsWith("M")) {
            multiplier = 1024L * 1024L;
        } else if (text.endsWith("GB") || text.endsWith("G")) {
            multiplier = 1024L * 1024L * 1024L;
        }
        String digits = multiplier == 1L ? text : text.replaceAll("[KMG]B?$", "");
        long number;
        try {
            number = Long.parseLong(digits.trim());
        } catch (NumberFormatException notANumber) {
            throw new IllegalArgumentException(operator + " [blockSize] must be a byte count,"
                    + " optionally suffixed K, M or G, but was '" + raw + "'");
        }
        if (number <= 0) {
            throw new IllegalArgumentException(
                    operator + " [blockSize] must be positive, but was '" + raw + "'");
        }
        return number * multiplier;
    }
}
