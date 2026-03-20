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
package com.teragrep.bor_01.tree;

import com.goterl.lazysodium.exceptions.SodiumException;
import com.goterl.lazysodium.interfaces.Ristretto255;
import com.teragrep.bor_01.metadata.Index;
import com.teragrep.bor_01.outbox.OutBox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map;
import java.util.Queue;

public class DiffUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(DiffUtil.class);

    public static class DiffResult {

        private final Index index;
        private final Instant instant;

        private DiffResult(Index index, Instant instant) {
            this.index = index;
            this.instant = instant;
        }

        public Index index() {
            return index;
        }

        public Instant instant() {
            return instant;
        }

        @Override
        public String toString() {
            return "DiffResult{" + "index=" + index + ", instant=" + instant + '}';
        }

    }

    public static Queue<DiffResult> compareTrees(OutBox remoteOutBox, OutBox localOutBox) throws SodiumException {
        // this is some nice little diff tree that drills down root->year->day->hour and feeds this queue
        LOGGER.debug("starting compareTrees");

        Queue<DiffResult> modifiedHourStarts = new LinkedList<>();

        Map<Index, MerkleTree> remoteSiteTrees = remoteOutBox.trees();

        for (Map.Entry<Index, MerkleTree> remoteSiteIndex2TreeEntry : remoteSiteTrees.entrySet()) {
            LOGGER.debug("comparing index <{}>", remoteSiteIndex2TreeEntry.getKey());

            // add index if missing
            if (!localOutBox.trees().containsKey(remoteSiteIndex2TreeEntry.getKey())) {
                localOutBox.addIndex(remoteSiteIndex2TreeEntry.getKey());
            }

            MerkleTree localSiteTree = localOutBox.trees().get(remoteSiteIndex2TreeEntry.getKey());

            if (!localSiteTree.root().equals(remoteSiteIndex2TreeEntry.getValue().root())) {
                LOGGER
                        .debug(
                                "roots differ remoteSite <{}> localSite <{}>",
                                remoteSiteIndex2TreeEntry.getValue().root(), localSiteTree.root()
                        );

                // find out changed years
                Map<Instant, Ristretto255.RistrettoPoint> remoteSiteYear2PointMap = remoteSiteIndex2TreeEntry
                        .getValue()
                        .years();
                Map<Instant, Ristretto255.RistrettoPoint> localSiteYear2PointMap = localSiteTree.years();

                for (
                    Map.Entry<Instant, Ristretto255.RistrettoPoint> remoteSiteYear2PointEntry : remoteSiteYear2PointMap
                            .entrySet()
                ) {

                    if (
                        !localSiteYear2PointMap.containsKey(remoteSiteYear2PointEntry.getKey())
                                || !localSiteYear2PointMap.get(remoteSiteYear2PointEntry.getKey()).equals(remoteSiteYear2PointEntry.getValue())
                    ) {
                        // year is modified
                        LOGGER.debug("year <{}> differs", remoteSiteYear2PointEntry.getKey());
                        for (Instant remoteSiteyearStart : remoteSiteYear2PointMap.keySet()) {
                            Map<Instant, Ristretto255.RistrettoPoint> remoteSiteDay2PointMap = remoteSiteIndex2TreeEntry
                                    .getValue()
                                    .days(remoteSiteyearStart);
                            Map<Instant, Ristretto255.RistrettoPoint> localSiteDay2PointMap = localSiteTree
                                    .days(remoteSiteyearStart);

                            for (
                                Map.Entry<Instant, Ristretto255.RistrettoPoint> remoteSiteDay2PointEntry : remoteSiteDay2PointMap
                                        .entrySet()
                            ) {
                                if (
                                    !localSiteDay2PointMap.containsKey(remoteSiteDay2PointEntry.getKey())
                                            || !localSiteDay2PointMap.get(remoteSiteDay2PointEntry.getKey()).equals(remoteSiteDay2PointEntry.getValue())
                                ) {
                                    // day is modified
                                    LOGGER.debug("day <{}> differs", remoteSiteDay2PointEntry.getKey());

                                    for (Instant remoteSitedayStart : remoteSiteDay2PointMap.keySet()) {

                                        Map<Instant, Ristretto255.RistrettoPoint> remoteSiteHour2PointMap = remoteSiteIndex2TreeEntry
                                                .getValue()
                                                .hours(remoteSitedayStart);
                                        Map<Instant, Ristretto255.RistrettoPoint> localSiteHour2PointMap = localSiteTree
                                                .hours(remoteSitedayStart);

                                        for (
                                            Map.Entry<Instant, Ristretto255.RistrettoPoint> remoteSiteHourEntry : remoteSiteHour2PointMap
                                                    .entrySet()
                                        ) {

                                            if (
                                                !localSiteHour2PointMap.containsKey(remoteSiteHourEntry.getKey())
                                                        || !localSiteHour2PointMap.get(remoteSiteHourEntry.getKey()).equals(remoteSiteHourEntry.getValue())
                                            ) {

                                                if (LOGGER.isDebugEnabled()) {
                                                    if (
                                                        localSiteHour2PointMap.containsKey(remoteSiteHourEntry.getKey())
                                                    ) {
                                                        LOGGER
                                                                .debug(
                                                                        "hour hex local <{}> remote <{}>",
                                                                        localSiteHour2PointMap
                                                                                .get(remoteSiteHourEntry.getKey())
                                                                                .toHex(),
                                                                        remoteSiteHourEntry.getValue().toHex()
                                                                );
                                                    }
                                                    else {
                                                        LOGGER
                                                                .debug(
                                                                        "hour <{}> not local, remote hex <{}>",
                                                                        remoteSiteHourEntry.getKey(),
                                                                        remoteSiteHourEntry.getValue().toHex()
                                                                );
                                                    }
                                                }
                                                // hour is modified and needs a pull
                                                LOGGER.debug("hour <{}> differs", remoteSiteHourEntry.getKey());
                                                // todo range pull i.e. 255 sub-ranges for the day
                                                modifiedHourStarts
                                                        .add(
                                                                new DiffResult(
                                                                        remoteSiteIndex2TreeEntry.getKey(),
                                                                        remoteSiteHourEntry.getKey()
                                                                )
                                                        );
                                            }

                                        }
                                    }

                                }

                            }
                        }

                    }
                }
            }
        }
        return modifiedHourStarts;
    }

}
