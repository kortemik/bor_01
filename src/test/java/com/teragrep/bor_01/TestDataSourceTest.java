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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.MalformedURLException;
import java.net.URL;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.*;

public class TestDataSourceTest {

    @Test
    public void test() throws SodiumException, NoSuchAlgorithmException, MalformedURLException {

        NavigableMap<Duration, Namespace> durationMap = new TreeMap<>();
        durationMap.put(Duration.ofSeconds(10L), new NamespaceFake("year-store"));

        TestDataSource testDataSource = new TestDataSource();

        LazySodiumJava lazySodiumJava = new LazySodiumJava(new SodiumJava());

        // site A
        OutBox outBoxA = new OutBoxImpl(lazySodiumJava);

        Storage storageA = new StorageImpl(new URL("https://localhost:8123/s3"), durationMap);

        MetadataStorage metadataStorageA = new MetadataStorageImpl();

        final long count = 1;
        long loops = 1;
        while (loops > 0) {
            Context context = testDataSource.get();

            //System.out.println(context);
            //System.out.println(context.metadata().point().toHex());

            outBoxA.objectFinalized(context.metadata());

            storageA
                    .put(
                            context.metadata().namespace(), context.metadata().path(), context.content(),
                            context.metadata().retention()
                    );

            outBoxA.objectStored(context.metadata());

            metadataStorageA.put(context.metadata());

            outBoxA.metadataStored(context.metadata());

            loops--;
        }

        // site B

        OutBox outBoxB = new OutBoxImpl(lazySodiumJava);

        Storage storageB = new StorageImpl(new URL("https://localhost:8080/s3"), durationMap);

        MetadataStorage metadataStorageB = new MetadataStorageImpl();

        Queue<DiffUtil.DiffResult> modifiedHourStarts = DiffUtil.compareTrees(outBoxA, outBoxB);
        Assertions.assertEquals(count, modifiedHourStarts.size());

        System.out.println("modifiedHourStarts: " + modifiedHourStarts);

        while (modifiedHourStarts.size() > 0) {
            DiffUtil.DiffResult diffResult = modifiedHourStarts.poll();
            List<Metadata> downloadManifest = metadataStorageA.get(diffResult.index(), diffResult.instant());
            System.out.println(downloadManifest);

            // download stuff
            for (Metadata metadataIn : downloadManifest) {
                // download to site B, perhaps mark as sync or so in the outbox while doing so or use some work scheduling
                byte[] contentIn = storageA.get(metadataIn.namespace(), metadataIn.path());
                // mark as ready
                outBoxB.objectFinalized(metadataIn);
                // store object on site B
                storageB.put(metadataIn.namespace(), metadataIn.path(), contentIn, metadataIn.retention());
                // mark as stored
                outBoxB.objectStored(metadataIn);
                // store metadata
                metadataStorageB.put(metadataIn);
                // mark as metadata stored
                outBoxB.metadataStored(metadataIn);
                // done
            }
        }

        Queue<DiffUtil.DiffResult> afterSyncDiffResult = DiffUtil.compareTrees(outBoxA, outBoxB);
        System.out.println("afterSyncDiffResult: " + afterSyncDiffResult);

        Assertions.assertEquals(0, afterSyncDiffResult.size());
    }

}
