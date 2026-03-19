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
import com.teragrep.bor_01.metadata.MetadataStorage;
import com.teragrep.bor_01.metadata.MetadataStorageImpl;
import com.teragrep.bor_01.objectstore.Namespace;
import com.teragrep.bor_01.objectstore.NamespaceFake;
import com.teragrep.bor_01.objectstore.Storage;
import com.teragrep.bor_01.objectstore.StorageImpl;
import com.teragrep.bor_01.outbox.OutBox;
import com.teragrep.bor_01.outbox.OutBoxImpl;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.MalformedURLException;
import java.net.URL;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;

public class TestDataSourceTest {

    @Test
    public void test() throws MalformedURLException {

        final long amountToRun = 1000L;

        ForkJoinPool pool = ForkJoinPool.commonPool();

        NavigableMap<Duration, Namespace> durationMap = new TreeMap<>();
        durationMap.put(Duration.ofSeconds(10L), new NamespaceFake("year-store"));
        LazySodiumJava lazySodiumJava = new LazySodiumJava(new SodiumJava());

        // site A
        TestDataSource testDataSourceA = new TestDataSource(lazySodiumJava, 0, "site-a");

        OutBox outBoxA = new OutBoxImpl(lazySodiumJava);

        Storage storageA = new StorageImpl(new URL("https://localhost:8123/s3"), durationMap);

        MetadataStorage metadataStorageA = new MetadataStorageImpl("siteA");

        Datacenter datacenterA = new Datacenter(
                "siteA",
                outBoxA,
                storageA,
                metadataStorageA,
                testDataSourceA,
                amountToRun
        );

        // site B

        TestDataSource testDataSourceB = new TestDataSource(lazySodiumJava, 1, "site-b");

        OutBox outBoxB = new OutBoxImpl(lazySodiumJava);

        Storage storageB = new StorageImpl(new URL("https://localhost:8080/s3"), durationMap);

        MetadataStorage metadataStorageB = new MetadataStorageImpl("siteB");

        Datacenter datacenterB = new Datacenter(
                "siteB",
                outBoxB,
                storageB,
                metadataStorageB,
                testDataSourceB,
                amountToRun
        );

        Reconciliation reconciliationDcA = new Reconciliation(
                "siteA",
                datacenterA.outBox(),
                datacenterA.storage(),
                datacenterA.metadataStorage(),
                datacenterB.outBox(),
                datacenterB.storage(),
                datacenterB.metadataStorage(),
                amountToRun
        );

        Reconciliation reconciliationDcB = new Reconciliation(
                "siteB",
                datacenterB.outBox(),
                datacenterB.storage(),
                datacenterB.metadataStorage(),
                datacenterA.outBox(),
                datacenterA.storage(),
                datacenterA.metadataStorage(),
                amountToRun
        );

        ForkJoinTask<Long> dcrAtask = pool.submit(reconciliationDcA);
        ForkJoinTask<Long> dcrBtask = pool.submit(reconciliationDcB);
        ForkJoinTask<Long> dcAtask = pool.submit(datacenterA);
        ForkJoinTask<Long> dcBtask = pool.submit(datacenterB);

        Assertions.assertDoesNotThrow(() -> {
            // TODO add timetouts
            long siteAgenerated = dcAtask.get();
            long siteBgenerated = dcBtask.get();
            long siteAreconcialted = dcrAtask.get();
            long siteBreconcialted = dcrBtask.get();

            Assertions.assertEquals(siteBgenerated, siteAreconcialted);
            Assertions.assertEquals(siteAgenerated, siteBreconcialted);
        });

        Assertions.assertEquals(amountToRun * 2, datacenterA.metadataStorage().size());
        Assertions.assertEquals(amountToRun * 2, datacenterB.metadataStorage().size());

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
