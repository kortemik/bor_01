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

import com.goterl.lazysodium.LazySodiumJava;
import com.goterl.lazysodium.exceptions.SodiumException;
import com.goterl.lazysodium.interfaces.Ristretto255;

import java.time.Instant;
import java.util.*;

// todo this tree computes everything on the fly
public class MerkleTreeImpl implements MerkleTree {

    private final LazySodiumJava lazySodiumJava;
    private final NavigableMap<Instant, Ristretto255.RistrettoPoint> hourPointMap;

    public MerkleTreeImpl(LazySodiumJava lazySodiumJava) {
        this(lazySodiumJava, new TreeMap<>());
    }

    public MerkleTreeImpl(
            LazySodiumJava lazySodiumJava,
            NavigableMap<Instant, Ristretto255.RistrettoPoint> hourPointMap
    ) {
        this.hourPointMap = hourPointMap;
        this.lazySodiumJava = lazySodiumJava;
    }

    @Override
    public synchronized boolean isStub() {
        return false;
    }

    @Override
    public synchronized void addMetadataPoint(final Instant epochHour, final Ristretto255.RistrettoPoint point)
            throws SodiumException {
        if (!hourPointMap.containsKey(epochHour)) {
            hourPointMap.put(epochHour, Ristretto255.RistrettoPoint.base(lazySodiumJava));
        }

        Ristretto255.RistrettoPoint pointFromTree = hourPointMap.get(epochHour);

        Ristretto255.RistrettoPoint newPoint = pointFromTree.plus(point);

        hourPointMap.put(epochHour, newPoint);
    }

    @Override
    public synchronized Ristretto255.RistrettoPoint root() throws SodiumException {

        NavigableMap<Instant, Ristretto255.RistrettoPoint> yearPointMap = years();

        Ristretto255.RistrettoPoint root = Ristretto255.RistrettoPoint.base(lazySodiumJava);

        for (Ristretto255.RistrettoPoint yearPoint : yearPointMap.values()) {
            root = root.plus(yearPoint);
        }

        return root;
    }

    @Override
    public synchronized NavigableMap<Instant, Ristretto255.RistrettoPoint> years() throws SodiumException {
        NavigableMap<Instant, Ristretto255.RistrettoPoint> yearPointMap = new TreeMap<>();

        for (Map.Entry<Instant, Ristretto255.RistrettoPoint> hourEntry : hourPointMap.entrySet()) {
            long epochYearLong = hourEntry.getKey().getEpochSecond() - hourEntry.getKey().getEpochSecond() % 31536000;
            Instant epochYear = Instant.ofEpochSecond(epochYearLong);

            if (!yearPointMap.containsKey(epochYear)) {
                yearPointMap.put(epochYear, Ristretto255.RistrettoPoint.base(lazySodiumJava));
            }

            Ristretto255.RistrettoPoint newYearPoint = yearPointMap.get(epochYear).plus(hourEntry.getValue());
            yearPointMap.put(epochYear, newYearPoint);
        }

        return yearPointMap;
    }

    @Override
    public synchronized NavigableMap<Instant, Ristretto255.RistrettoPoint> days(final Instant epochYearStart)
            throws SodiumException {
        long epochYearEndLong = epochYearStart.getEpochSecond() + 31536000;
        Instant epochYearEnd = Instant.ofEpochSecond(epochYearEndLong);

        NavigableMap<Instant, Ristretto255.RistrettoPoint> yearHourMap = hourPointMap
                .subMap(epochYearStart, true, epochYearEnd, false);

        NavigableMap<Instant, Ristretto255.RistrettoPoint> dayPointMap = new TreeMap<>();

        for (Map.Entry<Instant, Ristretto255.RistrettoPoint> hourEntry : yearHourMap.entrySet()) {
            long epochDayLong = hourEntry.getKey().getEpochSecond() - hourEntry.getKey().getEpochSecond() % 86400;
            Instant epochDay = Instant.ofEpochSecond(epochDayLong);

            if (!dayPointMap.containsKey(epochDay)) {
                dayPointMap.put(epochDay, Ristretto255.RistrettoPoint.base(lazySodiumJava));
            }

            Ristretto255.RistrettoPoint newDayPoint = dayPointMap.get(epochDay).plus(hourEntry.getValue());
            dayPointMap.put(epochDay, newDayPoint);
        }

        return dayPointMap;
    }

    @Override
    public synchronized NavigableMap<Instant, Ristretto255.RistrettoPoint> hours(final Instant epochDayStart)
            throws SodiumException {
        long epochDayEndLong = epochDayStart.getEpochSecond() + 86400;
        Instant epochDayEnd = Instant.ofEpochSecond(epochDayEndLong);

        return hourPointMap.subMap(epochDayStart, true, epochDayEnd, false);
    }

}
