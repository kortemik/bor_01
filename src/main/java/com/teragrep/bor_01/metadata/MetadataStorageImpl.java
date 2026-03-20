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
package com.teragrep.bor_01.metadata;

import com.teragrep.bor_01.id.IdImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

public class MetadataStorageImpl implements MetadataStorage {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetadataStorageImpl.class);

    private final String siteName;
    private final NavigableMap<RowKey, Metadata> store;
    private final Metadata metadataStub;

    public MetadataStorageImpl(String siteName) {
        this(siteName, new ConcurrentSkipListMap<>(), new MetadataStub());
    }

    private MetadataStorageImpl(String siteName, NavigableMap<RowKey, Metadata> store, Metadata metadataStub) {
        this.siteName = siteName;
        this.store = store;
        this.metadataStub = metadataStub;
    }

    @Override
    public synchronized void put(final Metadata metadata) {
        LOGGER.debug("about to put metadata <[{}]>", metadata);

        if (store.containsKey(metadata.rowKey())) {
            throw new IllegalArgumentException("Duplicate row key: " + metadata.rowKey());
        }

        store.put(metadata.rowKey(), metadata);
        LOGGER.debug("stored metadata <[{}]>", metadata);
        LOGGER.debug("metadata storage size <{}>", store.size());
    }

    @Override
    public synchronized Metadata get(final RowKey rowKey) {
        return store.getOrDefault(rowKey, metadataStub);
    }

    @Override
    public void delete(final RowKey rowKey) {

        if (!store.containsKey(rowKey)) {
            throw new IllegalArgumentException("Non-existing row key: " + rowKey);
        }

        store.remove(rowKey);
    }

    @Override
    public synchronized Collection<Metadata> get(Index index, Instant epochHourStart) {

        RowKey scanStartKey = new RowKeyImpl(
                index,
                epochHourStart,
                new IdImpl(Long.MIN_VALUE),
                new SiteImpl(Integer.MIN_VALUE, "")
        );
        RowKey scanEndKey = new RowKeyImpl(
                index,
                epochHourStart,
                new IdImpl(Long.MAX_VALUE),
                new SiteImpl(Integer.MAX_VALUE, "")
        );

        //LOGGER.info("about to subMap");
        // NOTE subMap is a subMap, not a copy
        return store.subMap(scanStartKey, scanEndKey).values();
        //LOGGER.info("submapped and got " + hourData.size() + " hours");
    }

    @Override
    public int size() {
        if (LOGGER.isDebugEnabled()) {
            Map<String, Long> siteToCount = new HashMap<>();
            for (Metadata metadata : store.values()) {
                if (!siteToCount.containsKey(metadata.site().name())) {
                    siteToCount.put(metadata.site().name(), 0L);
                }

                long count = siteToCount.get(metadata.site().name());
                count++;
                siteToCount.put(metadata.site().name(), count);
            }

            LOGGER.debug("siteName <{}> siteToCount <{}>", siteName, siteToCount);
        }
        return store.size();
    }

}
