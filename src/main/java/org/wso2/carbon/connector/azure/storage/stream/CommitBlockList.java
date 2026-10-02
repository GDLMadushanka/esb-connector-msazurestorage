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

import com.azure.storage.blob.models.BlobHttpHeaders;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.options.BlockBlobCommitBlockListOptions;
import com.azure.storage.blob.specialized.BlockBlobClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.stream.StreamCommit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Publishes the blob, or does not.
 *
 * <p>This is the whole of "a failed run publishes nothing" for a block blob, and it costs almost
 * nothing to honour. Until {@code Put Block List} runs the blob does not exist: staged blocks are
 * invisible to every reader, so there is no window in which anyone can see a half-written file and no
 * temporary name to rename. On failure there is literally nothing to undo — Azure reclaims the
 * orphaned blocks a week after the last write.
 *
 * <p>The list names <b>every</b> block from zero, not only the ones this attempt uploaded. A resumed
 * run commits blocks a previous attempt staged, which works because the ids are derived from the index
 * rather than remembered.
 */
final class CommitBlockList implements StreamCommit {

    private static final Log log = LogFactory.getLog(CommitBlockList.class);

    private final BlockBlobClient client;
    private final BlockIds ids;
    private final AzureBlobSettings settings;

    /** Not known until the stream ends, so the sink sets it before the scope unwinds. */
    private int blockCount = -1;

    CommitBlockList(BlockBlobClient client, BlockIds ids, AzureBlobSettings settings) {
        this.client = client;
        this.ids = ids;
        this.settings = settings;
    }

    void blockCount(int blockCount) {
        this.blockCount = blockCount;
    }

    /**
     * The success path: one operation, after which the blob exists complete.
     *
     * <p>A commit failure fails the transfer, which is correct — everything upstream reported success
     * on the understanding that this would happen.
     */
    @Override
    public void close() throws IOException {
        if (blockCount < 0) {
            throw new IOException("azure.blobStreamWrite reached its commit without a block count;"
                    + " the upload did not finish and the blob must not be committed");
        }
        if (blockCount == 0) {
            log.info("azure.blobStreamWrite: the source was empty, committing an empty blob");
        }
        List<String> ordered = new ArrayList<>(blockCount);
        for (int index = 0; index < blockCount; index++) {
            ordered.add(ids.idFor(index));
        }
        BlockBlobCommitBlockListOptions options = new BlockBlobCommitBlockListOptions(ordered);
        if (settings.contentType() != null) {
            options.setHeaders(new BlobHttpHeaders().setContentType(settings.contentType()));
        }
        if (!settings.overwrite()) {
            // "*" means "only if the blob does not already exist"; a pre-existing blob fails the run
            // rather than being replaced.
            options.setRequestConditions(new BlobRequestConditions().setIfNoneMatch("*"));
        }
        try {
            client.commitBlockListWithResponse(options, null, null);
        } catch (RuntimeException failed) {
            throw new IOException("azure.blobStreamWrite could not commit " + blockCount
                    + " blocks to '" + settings.container() + "/" + settings.blob() + "': "
                    + failed.getMessage(), failed);
        }
        log.info("azure.blobStreamWrite committed " + blockCount + " blocks to '"
                + settings.container() + "/" + settings.blob() + "'");
    }

    /**
     * The failure path. Deliberately empty.
     *
     * <p>Runs while a failure is already being reported, so nothing here may throw. Nothing needs to
     * happen either: the blocks are uncommitted, so no reader ever saw them, and leaving them is what
     * lets the next attempt resume instead of starting again.
     */
    @Override
    public void abort() {
        log.info("azure.blobStreamWrite aborted; '" + settings.container() + "/" + settings.blob()
                + "' was never committed. Staged blocks are kept for the next attempt to resume from.");
    }
}
