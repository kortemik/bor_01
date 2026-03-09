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

        long count = 1;
        while (count > 0) {
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

            count--;
        }

        // site B
        // once in 5 seconds
        // requires list of indexes for trees
        // get local tree roots, get remote tree roots
        // check validity, if not, drill down to applicaple ranges
        // create and insert missing metadata to local

        OutBox outBoxB = new OutBoxImpl(lazySodiumJava);

        Storage storageB = new StorageImpl(new URL("https://localhost:8080/s3"), durationMap);

        MetadataStorage metadataStorageB = new MetadataStorageImpl();

        Queue<DiffUtil.DiffResult> modifiedHourStarts = DiffUtil.compareTrees(outBoxA, outBoxB);

        System.out.println("modifiedHourStarts: " + modifiedHourStarts);
    }

}
