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
import com.goterl.lazysodium.interfaces.Ristretto255;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Ristretto255Test {

    @Test
    public void testAddSubstract() throws SodiumException, NoSuchAlgorithmException {

        // bogus sha256
        MessageDigest md256 = MessageDigest.getInstance("SHA-256");
        byte[] sha256Digest = md256.digest("foobar".getBytes(StandardCharsets.UTF_8));

        // expand to sha512 as ristretto requires 64bit points
        MessageDigest md512 = MessageDigest.getInstance("SHA-512");
        byte[] expanded = md512.digest(sha256Digest);

        LazySodiumJava ls = new LazySodiumJava(new SodiumJava());

        Ristretto255.RistrettoPoint base = Ristretto255.RistrettoPoint.base(ls);

        Ristretto255.RistrettoPoint point = ls.cryptoCoreRistretto255FromHash(expanded);

        Ristretto255.RistrettoPoint added = ls.cryptoCoreRistretto255Add(base, point);

        Ristretto255.RistrettoPoint result = ls.cryptoCoreRistretto255Sub(added, point);

        Assertions.assertEquals(base, result);
    }

    @Test
    @EnabledIfSystemProperty(
            named = "testRistretto255Performance",
            matches = "true"
    )
    public void testPerformance() throws SodiumException, NoSuchAlgorithmException {

        // bogus sha256
        MessageDigest md256 = MessageDigest.getInstance("SHA-256");
        byte[] sha256Digest = md256.digest("foobar".getBytes(StandardCharsets.UTF_8));

        // expand to sha512 as ristretto requires 64bit points
        MessageDigest md512 = MessageDigest.getInstance("SHA-512");
        byte[] expanded = md512.digest(sha256Digest);

        LazySodiumJava ls = new LazySodiumJava(new SodiumJava());

        final long amount = 1000000;
        long counter = 0;

        long start = System.nanoTime();
        while (counter < amount) {

            // modify in-hash so that jvm can't optimize the result of mapping
            expanded[0] = (byte) (counter >> 24);
            expanded[1] = (byte) (counter >> 16);
            expanded[2] = (byte) (counter >> 8);
            expanded[3] = (byte) counter;

            Ristretto255.RistrettoPoint point = ls.cryptoCoreRistretto255FromHash(expanded);

            if (point == null) {
                throw new RuntimeException("Mapping failed");
            }
            counter++;
        }
        long end = System.nanoTime();

        long timeTaken = (end - start) / 1_000_000;
        float hashesPerMilliSecond = (float) counter / timeTaken;
        System.out.println("hashesPerMilliSecond " + hashesPerMilliSecond);
        // got 58.558296 on E-2276M
    }
}
