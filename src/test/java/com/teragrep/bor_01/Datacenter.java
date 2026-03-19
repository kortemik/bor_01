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
import com.teragrep.bor_01.metadata.MetadataStorage;
import com.teragrep.bor_01.object.Context;
import com.teragrep.bor_01.objectstore.Storage;
import com.teragrep.bor_01.outbox.OutBox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.NoSuchAlgorithmException;
import java.util.concurrent.Callable;

class Datacenter implements Callable<Long>, Connectivity {

    private static final Logger LOGGER = LoggerFactory.getLogger(Datacenter.class);

    private final String siteName;
    private final OutBox outBox;
    private final Storage storage;
    private final MetadataStorage metadataStorage;
    private final TestDataSource testDataSource;
    private final long count;

    Datacenter(
            String siteName,
            OutBox outbox,
            Storage storage,
            MetadataStorage metadataStorage,
            TestDataSource testDataSource,
            long count
    ) {
        this.siteName = siteName;
        this.outBox = outbox;
        this.storage = storage;
        this.metadataStorage = metadataStorage;
        this.testDataSource = testDataSource;
        this.count = count;
    }

    @Override
    public Long call() {
        long counter = 0;

        try {
            while (counter < count) {
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
                    counter++;
                }
                catch (SodiumException | NoSuchAlgorithmException e) {
                    LOGGER.error("Exception ", e);
                    break;
                }

                try {
                    Thread.sleep(0L);
                }
                catch (InterruptedException ignored) {

                }
            }
        }
        catch (Exception e) {
            LOGGER.error("Exception ", e);
        }
        return counter;
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
