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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.OptionalInt;

/**
 * Block ids for a chunked blob upload, <b>derived from the index rather than stored</b>.
 *
 * <p>Azure commits a block blob by being handed the ordered list of its block ids. A resumed attempt
 * therefore has to be able to say which id held which byte range, and it has to do so after a crash
 * that kept nothing in memory. Two ways exist: record the list, or derive it. Recording is not
 * available — {@code Checkpoint.MAX_PAYLOAD_BYTES} is 4096 and fifty thousand ids are about a
 * megabyte — so the id is a pure function of the index and the name <i>is</i> the position.
 *
 * <p>This is also why the SDK's own uploader cannot be used for a resumable transfer: it names blocks
 * with {@code UUID.randomUUID()}, and a bag of random names cannot be mapped back to byte ranges.
 *
 * <h2>Three rules, none of them enforced by the compiler</h2>
 * <ol>
 *   <li><b>Every id for one blob must encode to the same length.</b> Azure rejects a blob whose
 *       blocks have mixed id lengths with 400. The index is therefore zero-padded to a fixed width
 *       and the run tag is a fixed-width hash, so every id is exactly {@link #ENCODED_WIDTH} chars.</li>
 *   <li><b>The index must survive the round trip, because the returned order cannot be trusted.</b>
 *       {@code Get Block List} does return the uncommitted list alphabetically, and it is tempting to
 *       walk it as-is — but the ids are Base64, and <b>Base64 is not order-preserving</b>. Its
 *       alphabet runs {@code A-Z a-z 0-9}, so an encoded {@code 049999} sorts <i>before</i> an encoded
 *       {@code 000099}. Zero-padding orders the plaintext, not the encoding. The resume scan therefore
 *       decodes every id with {@link #indexOf(String)} and walks its own indices; the padding is kept
 *       for rule 1 and for a fixed-width parse, not for sorting.</li>
 *   <li><b>The tag must be reproducible by the next attempt.</b> It is derived from the job id,
 *       which {@code JobContext.jobId()} documents as "stable across retries of the same logical
 *       transfer". A fresh tag per attempt would leave the previous attempt's blocks unfindable and
 *       silently restart every resume from zero.</li>
 * </ol>
 *
 * <h2>When there is no job id</h2>
 * {@code jobId()} may legitimately return {@code noop}, which every such run shares. Deriving the tag
 * from it would give two concurrent uploads to the same blob <i>identical</i> block ids, interleaving
 * two streams into one blob — corruption rather than a last-writer-wins race. A run with no job id
 * also cannot resume, so nothing is lost by making its tag random per attempt, and that is what
 * {@link #forJob(String)} does.
 *
 * <p>Instances are immutable and safe to share.
 */
public final class BlockIds {

    /** Marks our ids in a block list a human is reading, and lets the scan ignore anyone else's. */
    private static final String PREFIX = "mft-";

    /** Hex chars of the run tag. 32 bits of job id; a collision costs one confused blob, not silence. */
    private static final int TAG_CHARS = 8;

    /** Azure allows 50,000 committed and 100,000 uncommitted blocks, so six digits is ample. */
    private static final int INDEX_DIGITS = 6;

    /** Largest index this scheme can name. The 50,000-block Azure ceiling bites long before it. */
    public static final int MAX_INDEX = 999_999;

    /** Every id encodes to exactly this many characters. Rule 1 above depends on it. */
    public static final int ENCODED_WIDTH =
            ((PREFIX.length() + TAG_CHARS + 1 + INDEX_DIGITS) + 2) / 3 * 4;

    private static final String INDEX_FORMAT = "%0" + INDEX_DIGITS + "d";

    private final String tag;
    private final String plainPrefix;

    private BlockIds(String tag) {
        this.tag = tag;
        this.plainPrefix = PREFIX + tag + "-";
    }

    /**
     * @param jobId {@code JobContext.jobId()}; {@code null}, blank or {@code noop} all mean "no id",
     *              which yields a random tag because such a run can never resume anyway
     */
    public static BlockIds forJob(String jobId) {
        boolean real = jobId != null && !jobId.isBlank() && !"noop".equals(jobId);
        return new BlockIds(real ? hash(jobId) : randomTag());
    }

    /** The id for block {@code index}, Base64 as Azure requires, always {@link #ENCODED_WIDTH} chars. */
    public String idFor(int index) {
        if (index < 0 || index > MAX_INDEX) {
            throw new IllegalArgumentException("block index " + index + " is outside 0.." + MAX_INDEX);
        }
        String plain = plainPrefix + String.format(INDEX_FORMAT, index);
        return Base64.getEncoder().encodeToString(plain.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * The index an id names, or empty when the id is not one of ours — a block staged by another run,
     * by an earlier scheme, or by something else entirely. The resume scan must skip those rather than
     * count them, or it would claim a prefix it never wrote.
     */
    public OptionalInt indexOf(String encodedId) {
        if (encodedId == null || encodedId.length() != ENCODED_WIDTH) {
            return OptionalInt.empty();
        }
        String plain;
        try {
            plain = new String(Base64.getDecoder().decode(encodedId), StandardCharsets.UTF_8);
        } catch (IllegalArgumentException notBase64) {
            return OptionalInt.empty();
        }
        if (!plain.startsWith(plainPrefix) || plain.length() != plainPrefix.length() + INDEX_DIGITS) {
            return OptionalInt.empty();
        }
        try {
            return OptionalInt.of(Integer.parseInt(plain.substring(plainPrefix.length())));
        } catch (NumberFormatException notANumber) {
            return OptionalInt.empty();
        }
    }

    /** The run tag, for log lines that need to say which attempt's blocks are on the service. */
    public String tag() {
        return tag;
    }

    private static String hash(String value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder(TAG_CHARS);
            for (int i = 0; i < TAG_CHARS / 2; i++) {
                hex.append(String.format("%02x", digest[i]));
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is required of every JVM", impossible);
        }
    }

    private static String randomTag() {
        byte[] bytes = new byte[TAG_CHARS / 2];
        new SecureRandom().nextBytes(bytes);
        StringBuilder hex = new StringBuilder(TAG_CHARS);
        for (byte b : bytes) {
            hex.append(String.format("%02x", b));
        }
        return hex.toString();
    }
}
