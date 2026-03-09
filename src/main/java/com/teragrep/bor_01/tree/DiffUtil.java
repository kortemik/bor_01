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

import java.time.Instant;
import java.util.LinkedList;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Queue;

public class DiffUtil {

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

    public static Queue<DiffResult> compareTrees(OutBox outBoxA, OutBox outBoxB) throws SodiumException {
        // this is some nice little diff tree that drills down root->year->day->hour and feeds this queue

        Queue<DiffResult> modifiedHourStarts = new LinkedList<>();

        Map<Index, MerkleTree> siteATrees = outBoxA.trees();

        for (Map.Entry<Index, MerkleTree> siteAIndex2TreeEntry : siteATrees.entrySet()) {

            // add index if missing
            if (!outBoxB.trees().containsKey(siteAIndex2TreeEntry.getKey())) {
                outBoxB.addIndex(siteAIndex2TreeEntry.getKey());
            }

            MerkleTree siteBTree = outBoxB.trees().get(siteAIndex2TreeEntry.getKey());

            if (!siteBTree.root().equals(siteAIndex2TreeEntry.getValue().root())) {
                System.out.println("there some difference");

                // find out changed years
                NavigableMap<Instant, Ristretto255.RistrettoPoint> siteAYear2PointMap = siteAIndex2TreeEntry
                        .getValue()
                        .years();
                NavigableMap<Instant, Ristretto255.RistrettoPoint> siteBYear2PointMap = siteBTree.years();

                for (
                    Map.Entry<Instant, Ristretto255.RistrettoPoint> siteAYear2PointEntry : siteAYear2PointMap.entrySet()
                ) {

                    if (
                        !siteBYear2PointMap.containsKey(siteAYear2PointEntry.getKey()) || siteBYear2PointMap
                                .get(siteAYear2PointEntry.getKey())
                                .equals(siteAYear2PointEntry.getValue())
                    ) {
                        // year is modified

                        for (Instant siteAyearStart : siteAYear2PointMap.keySet()) {
                            NavigableMap<Instant, Ristretto255.RistrettoPoint> siteADay2PointMap = siteAIndex2TreeEntry
                                    .getValue()
                                    .days(siteAyearStart);
                            NavigableMap<Instant, Ristretto255.RistrettoPoint> siteBDay2PointMap = siteBTree
                                    .days(siteAyearStart);

                            for (
                                Map.Entry<Instant, Ristretto255.RistrettoPoint> siteADay2PointEntry : siteADay2PointMap
                                        .entrySet()
                            ) {
                                if (
                                    !siteBDay2PointMap.containsKey(siteADay2PointEntry.getKey()) || siteBDay2PointMap
                                            .get(siteADay2PointEntry.getKey())
                                            .equals(siteADay2PointEntry.getValue())
                                ) {
                                    // day is modified

                                    for (Instant siteAdayStart : siteADay2PointMap.keySet()) {

                                        NavigableMap<Instant, Ristretto255.RistrettoPoint> siteAHour2PointMap = siteAIndex2TreeEntry
                                                .getValue()
                                                .hours(siteAdayStart);
                                        NavigableMap<Instant, Ristretto255.RistrettoPoint> siteBHour2PointMap = siteBTree
                                                .hours(siteAdayStart);

                                        for (
                                            Map.Entry<Instant, Ristretto255.RistrettoPoint> siteAHourEntry : siteAHour2PointMap
                                                    .entrySet()
                                        ) {

                                            if (
                                                !siteBDay2PointMap
                                                        .containsKey(siteAHourEntry.getKey()) || siteBHour2PointMap
                                                                .get(siteAHourEntry.getKey())
                                                                .equals(siteAHourEntry.getValue())
                                            ) {
                                                // hour is modified and needs a pull
                                                // todo range pull i.e. 255 sub-ranges for the day
                                                modifiedHourStarts
                                                        .add(
                                                                new DiffResult(
                                                                        siteAIndex2TreeEntry.getKey(),
                                                                        siteAHourEntry.getKey()
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
