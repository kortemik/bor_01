/*
 * Binary Object Replication for Teragrep (bor_01)
 * Copyright (C) 2026 Suomen Kanuuna Oy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 *
 * Additional permission under GNU Affero General Public License version 3
 * section 7
 *
 * If you modify this Program, or any covered work, by linking or combining it
 * with other code, such other code is not for that reason alone subject to any
 * of the requirements of the GNU Affero GPL version 3 as long as this Program
 * is the same Program as licensed from Suomen Kanuuna Oy without any additional
 * modifications.
 *
 * Supplemented terms under GNU Affero General Public License version 3
 * section 7
 *
 * Origin of the software must be attributed to Suomen Kanuuna Oy. Any modified
 * versions must be marked as "Modified version of" The Program.
 *
 * Names of the licensors and authors may not be used for publicity purposes.
 *
 * No rights are granted for use of trade names, trademarks, or service marks
 * which are in The Program if any.
 *
 * Licensee must indemnify licensors and authors for any liability that these
 * contractual assumptions impose on licensors and authors.
 *
 * To the extent this program is licensed as part of the Commercial versions of
 * Teragrep, the applicable Commercial License may apply to this file if you as
 * a licensee so wish it.
 */
package com.teragrep.bor_01;

import com.goterl.lazysodium.exceptions.SodiumException;
import com.teragrep.bor_01.metadata.Metadata;
import com.teragrep.bor_01.metadata.MetadataStorage;
import com.teragrep.bor_01.objectstore.Storage;
import com.teragrep.bor_01.outbox.OutBox;
import com.teragrep.bor_01.tree.DiffUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;

class Reconciliation implements Callable<Long> {

    private static final Logger LOGGER = LoggerFactory.getLogger(Reconciliation.class);

    private final String siteName;
    private final OutBox localOutBox;
    private final Storage localStorage;
    private final MetadataStorage localMetadataStorage;

    private final OutBox remoteOutBox;
    private final Storage remoteStorage;
    private final MetadataStorage remoteMetadataStorage;

    private final long expectedCount;

    Reconciliation(
            String siteName,
            OutBox localOutBox,
            Storage localStorage,
            MetadataStorage localMetadataStorage,
            OutBox remoteOutBox,
            Storage remoteStorage,
            MetadataStorage remoteMetadataStorage,
            long expectedCount
    ) {
        this.siteName = siteName;
        this.localOutBox = localOutBox;
        this.localStorage = localStorage;
        this.localMetadataStorage = localMetadataStorage;

        this.remoteOutBox = remoteOutBox;
        this.remoteStorage = remoteStorage;
        this.remoteMetadataStorage = remoteMetadataStorage;

        this.expectedCount = expectedCount;
    }

    @Override
    public Long call() {
        long counter = 0;
        try {
            while (counter < expectedCount) {
                try {
                    Queue<DiffUtil.DiffResult> modifiedHourStarts = DiffUtil.compareTrees(remoteOutBox, localOutBox);

                    LOGGER.debug("siteName <{}> modifiedHourStarts size <{}>", siteName, modifiedHourStarts.size());

                    while (!modifiedHourStarts.isEmpty()) {
                        DiffUtil.DiffResult diffResult = modifiedHourStarts.poll();
                        LOGGER.debug("about to download metadata for <{}>", diffResult);

                        List<Metadata> downloadManifest = remoteMetadataStorage
                                .get(diffResult.index(), diffResult.instant());
                        LOGGER.debug("downloadManifest <{}>", downloadManifest);

                        // ++ TODO DO CROSS CHECK IF WE ALREADY HAVE SOME OF THE SET

                        Set<Metadata> downloadedSet = new HashSet<>(downloadManifest);

                        List<Metadata> localManifest = localMetadataStorage
                                .get(diffResult.index(), diffResult.instant());
                        Set<Metadata> localSet = new HashSet<>(localManifest);

                        downloadedSet.removeAll(localSet);
                        // --

                        // download stuff
                        for (Metadata metadataIn : downloadedSet) {
                            // download to site B, perhaps mark as sync or so in the outbox while doing so or use some work scheduling
                            LOGGER.debug("about to download <{}>", metadataIn);
                            byte[] contentIn = remoteStorage.get(metadataIn.namespace(), metadataIn.path());
                            LOGGER.debug("downloadded <{}>", metadataIn);
                            // mark as ready
                            localOutBox.objectFinalized(metadataIn);
                            LOGGER.debug("object finalized <{}>", metadataIn);
                            // store object on site B
                            localStorage
                                    .put(metadataIn.namespace(), metadataIn.path(), contentIn, metadataIn.retention());
                            LOGGER.debug("object stored <{}>", metadataIn);
                            // mark as stored
                            localOutBox.objectStored(metadataIn);
                            LOGGER.debug("object marked stored <{}>", metadataIn);
                            // store metadata
                            localMetadataStorage.put(metadataIn);
                            LOGGER.debug("object metadata stored <{}>", metadataIn);
                            // mark as metadata stored
                            localOutBox.metadataStored(metadataIn);
                            // done
                            LOGGER.info("siteName <{}> metadataIn <{}>", siteName, metadataIn);
                            counter++;
                        }
                    }

                    LOGGER.debug("siteName <{}> metadataStorage.size <{}>", siteName, localMetadataStorage.size());

                    Thread.sleep(1000L);
                }
                catch (SodiumException | NoSuchAlgorithmException e) {
                    LOGGER.error("Exception ", e);
                    break;
                }
                catch (InterruptedException ignored) {

                }
                LOGGER.debug("rerunning sync loop");
            }
            LOGGER.info("exiting at thread <{}>", Thread.currentThread().getName());
        }
        catch (Exception e) {
            LOGGER.error("Exception ", e);
        }
        return counter;
    }

}
