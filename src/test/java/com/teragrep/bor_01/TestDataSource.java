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
import com.teragrep.bor_01.id.Allocation;
import com.teragrep.bor_01.id.AllocationImpl;
import com.teragrep.bor_01.metadata.*;
import com.teragrep.bor_01.object.Context;
import com.teragrep.bor_01.object.ContextImpl;
import com.teragrep.bor_01.objectstore.Namespace;
import com.teragrep.bor_01.objectstore.NamespaceFake;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

public final class TestDataSource implements Supplier<Context> {

    private final LazySodiumJava lazySodiumJava;
    private final Index index;
    private final Site site;
    private final Allocation allocation;
    private final Namespace namespace;
    private final String testContentPrefix;

    public TestDataSource(LazySodiumJava lazySodiumJava, int siteId, String siteName) {
        this.lazySodiumJava = lazySodiumJava;
        this.index = new IndexFake();
        this.site = new SiteFake(siteId, siteName);
        this.allocation = new AllocationImpl();
        this.namespace = new NamespaceFake("year-store");
        this.testContentPrefix = "test data at Instant.now() -> ";

    }

    @Override
    public Context get() {
        long randomNum = ThreadLocalRandom.current().nextLong(0, Instant.now().getEpochSecond());
        Instant now = Instant.ofEpochSecond(0).plusSeconds(randomNum);
        String content = testContentPrefix + now;
        byte[] contentBytes = content.getBytes(StandardCharsets.UTF_8);

        // reusing same digest instance and .reset() it is preferred way
        final MessageDigest md256;
        try {
            md256 = MessageDigest.getInstance("SHA-256");
        }
        catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }

        byte[] contentSha256 = md256.digest(contentBytes);

        String path = "/some/random/path/" + UUID.randomUUID();

        long epochHourLong = now.getEpochSecond() - now.getEpochSecond() % 3600;
        Instant epochHour = Instant.ofEpochSecond(epochHourLong);

        Metadata metadata = new MetadataImpl(
                lazySodiumJava,
                index,
                site,
                allocation.get(),
                epochHour,
                Duration.ofSeconds(10),
                namespace,
                Path.of(path),
                contentSha256
        );

        return new ContextImpl(metadata, contentBytes);
    }

}
