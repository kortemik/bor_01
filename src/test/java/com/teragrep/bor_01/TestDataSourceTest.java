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

import com.goterl.lazysodium.LazySodiumJava;
import com.goterl.lazysodium.SodiumJava;
import com.goterl.lazysodium.exceptions.SodiumException;
import com.teragrep.bor_01.metadata.Metadata;
import com.teragrep.bor_01.metadata.MetadataStorage;
import com.teragrep.bor_01.metadata.MetadataStorageImpl;
import com.teragrep.bor_01.object.Context;
import com.teragrep.bor_01.objectstore.Namespace;
import com.teragrep.bor_01.objectstore.NamespaceFake;
import com.teragrep.bor_01.objectstore.Storage;
import com.teragrep.bor_01.objectstore.StorageImpl;
import com.teragrep.bor_01.outbox.OutBox;
import com.teragrep.bor_01.outbox.OutBoxImpl;
import com.teragrep.bor_01.tree.DiffUtil;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;

public class TestDataSourceTest {

    interface Connectivity {

        OutBox outBox();

        Storage storage();

        MetadataStorage metadataStorage();

    }

    private static class Datacenter implements Runnable, Connectivity {

        private static final Logger LOGGER = LoggerFactory.getLogger(Datacenter.class);

        private final String siteName;
        private final OutBox outBox;
        private final Storage storage;
        private final MetadataStorage metadataStorage;
        private final TestDataSource testDataSource;

        Datacenter(
                String siteName,
                OutBox outbox,
                Storage storage,
                MetadataStorage metadataStorage,
                TestDataSource testDataSource
        ) {
            this.siteName = siteName;
            this.outBox = outbox;
            this.storage = storage;
            this.metadataStorage = metadataStorage;
            this.testDataSource = testDataSource;
        }

        @Override
        public void run() {
            try {
                while (true) {
                    Context context = testDataSource.get();

                    try {
                        LOGGER.debug("site <{}> new data hex <{}>", siteName, context.metadata().point().toHex());
                    }
                    catch (NoSuchAlgorithmException | SodiumException e) {
                        throw new RuntimeException(e);
                    }

                    outBox.objectFinalized(context.metadata());

                    storage
                            .put(
                                    context.metadata().namespace(), context.metadata().path(), context.content(),
                                    context.metadata().retention()
                            );

                    outBox.objectStored(context.metadata());

                    metadataStorage.put(context.metadata());

                    try {
                        outBox.metadataStored(context.metadata());
                    }
                    catch (SodiumException | NoSuchAlgorithmException e) {
                        LOGGER.error("Exception ", e);
                        break;
                    }

                    try {
                        Thread.sleep(20L);
                    }
                    catch (InterruptedException ignored) {

                    }
                }
            }
            catch (Exception e) {
                LOGGER.error("Exception ", e);
            }
        }

        @Override
        public OutBox outBox() {
            return outBox;
        }

        @Override
        public Storage storage() {
            return storage;
        }

        @Override
        public MetadataStorage metadataStorage() {
            return metadataStorage;
        }

    }

    private static class Reconciliation implements Runnable {

        private static final Logger LOGGER = LoggerFactory.getLogger(Reconciliation.class);

        private final String siteName;
        private final OutBox localOutBox;
        private final Storage localStorage;
        private final MetadataStorage localMetadataStorage;

        private final OutBox remoteOutBox;
        private final Storage remoteStorage;
        private final MetadataStorage remoteMetadataStorage;

        Reconciliation(
                String siteName,
                OutBox localOutBox,
                Storage localStorage,
                MetadataStorage localMetadataStorage,
                OutBox remoteOutBox,
                Storage remoteStorage,
                MetadataStorage remoteMetadataStorage
        ) {
            this.siteName = siteName;
            this.localOutBox = localOutBox;
            this.localStorage = localStorage;
            this.localMetadataStorage = localMetadataStorage;

            this.remoteOutBox = remoteOutBox;
            this.remoteStorage = remoteStorage;
            this.remoteMetadataStorage = remoteMetadataStorage;

        }

        @Override
        public void run() {
            try {
                while (true) {
                    try {
                        Queue<DiffUtil.DiffResult> modifiedHourStarts = DiffUtil
                                .compareTrees(remoteOutBox, localOutBox);

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
                                        .put(
                                                metadataIn.namespace(), metadataIn.path(), contentIn,
                                                metadataIn.retention()
                                        );
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
                            }
                        }

                        LOGGER.debug("siteName <{}> metadataStorage.size <{}>", siteName, localMetadataStorage.size());

                        Thread.sleep(5000L);
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
        }

    }

    @Test
    public void test() throws MalformedURLException {

        ForkJoinPool pool = ForkJoinPool.commonPool();

        NavigableMap<Duration, Namespace> durationMap = new TreeMap<>();
        durationMap.put(Duration.ofSeconds(10L), new NamespaceFake("year-store"));
        LazySodiumJava lazySodiumJava = new LazySodiumJava(new SodiumJava());

        // site A
        TestDataSource testDataSourceA = new TestDataSource(lazySodiumJava, 0, "site-a");

        OutBox outBoxA = new OutBoxImpl(lazySodiumJava);

        Storage storageA = new StorageImpl(new URL("https://localhost:8123/s3"), durationMap);

        MetadataStorage metadataStorageA = new MetadataStorageImpl("siteA");

        Datacenter datacenterA = new Datacenter("siteA", outBoxA, storageA, metadataStorageA, testDataSourceA);

        // site B

        TestDataSource testDataSourceB = new TestDataSource(lazySodiumJava, 1, "site-b");

        OutBox outBoxB = new OutBoxImpl(lazySodiumJava);

        Storage storageB = new StorageImpl(new URL("https://localhost:8080/s3"), durationMap);

        MetadataStorage metadataStorageB = new MetadataStorageImpl("siteB");

        Datacenter datacenterB = new Datacenter("siteB", outBoxB, storageB, metadataStorageB, testDataSourceB);

        Reconciliation reconciliationDcA = new Reconciliation(
                "siteA",
                datacenterA.outBox(),
                datacenterA.storage(),
                datacenterA.metadataStorage(),
                datacenterB.outBox(),
                datacenterB.storage(),
                datacenterB.metadataStorage()
        );

        Reconciliation reconciliationDcB = new Reconciliation(
                "siteB",
                datacenterB.outBox(),
                datacenterB.storage(),
                datacenterB.metadataStorage(),
                datacenterA.outBox(),
                datacenterA.storage(),
                datacenterA.metadataStorage()
        );

        ForkJoinTask<?> dcrAtask = pool.submit(reconciliationDcA);
        ForkJoinTask<?> dcrBtask = pool.submit(reconciliationDcB);
        ForkJoinTask<?> dcAtask = pool.submit(datacenterA);
        ForkJoinTask<?> dcBtask = pool.submit(datacenterB);

        try {
            dcBtask.get();
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
        /*
        Assertions.assertEquals(count, modifiedHourStarts.size());
        
        System.out.println("modifiedHourStarts: " + modifiedHourStarts);
        
        
        
        Queue<DiffUtil.DiffResult> afterSyncDiffResult = DiffUtil.compareTrees(outBoxA, outBoxB);
        System.out.println("afterSyncDiffResult: " + afterSyncDiffResult);
        
        Assertions.assertEquals(0, afterSyncDiffResult.size());
        
         */

        // todo handle expiration, atm objects are removed once in a day, so perhaps mark as tombstone and skip from point calculation

        // todo handle issue where diffResult returns metadata that is in transit

    }

}
