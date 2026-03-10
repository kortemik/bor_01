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

import com.goterl.lazysodium.interfaces.Ristretto255;
import com.teragrep.bor_01.id.Id;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;

import com.goterl.lazysodium.LazySodiumJava;
import com.goterl.lazysodium.exceptions.SodiumException;
import com.teragrep.bor_01.objectstore.Namespace;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

public class MetadataImpl implements Metadata {

    private final LazySodiumJava lazySodiumJava;
    private final Index index;
    private final Site site;
    private final Id id;
    private final Instant epochHour;
    private final Duration retention;
    private final Namespace namespace;
    private final Path path;
    private final byte[] sha256;

    public MetadataImpl(
            LazySodiumJava lazySodiumJava,
            Index index,
            Site site,
            Id id,
            Instant epochHour,
            Duration retention,
            Namespace namespace,
            Path path,
            byte[] sha256
    ) {
        this.lazySodiumJava = lazySodiumJava;
        this.index = index;
        this.site = site;
        this.id = id;
        this.epochHour = epochHour;
        this.retention = retention;
        this.namespace = namespace;
        this.path = path;
        this.sha256 = sha256;
    }

    @Override
    public Index index() {
        return index;
    }

    @Override
    public Site site() {
        return site;
    }

    @Override
    public Id id() {
        return id;
    }

    @Override
    public Instant epochHour() {
        return epochHour;
    }

    @Override
    public Duration retention() {
        return retention;
    }

    @Override
    public Namespace namespace() {
        return namespace;
    }

    @Override
    public Path path() {
        return path;
    }

    @Override
    public byte[] sha256() {
        return sha256;
    }

    @Override
    public Ristretto255.RistrettoPoint point() throws NoSuchAlgorithmException, SodiumException {
        // reusing same digest instance and .reset() it is preferred way
        MessageDigest md512 = MessageDigest.getInstance("SHA-512");
        byte[] metadataSha512 = md512.digest(asBytes());

        return lazySodiumJava.cryptoCoreRistretto255FromHash(metadataSha512);
    }

    @Override
    public RowKey rowKey() {
        return new RowKeyImpl(index, epochHour, id, site);
    }

    @Override
    public boolean isStub() {
        return false;
    }

    @Override
    public byte[] asBytes() {

        ByteBuffer buffer = ByteBuffer
                .allocate(
                        Long.BYTES
                                + Long.BYTES + Long.BYTES + Long.BYTES + Long.BYTES + namespace
                                        .name()
                                        .getBytes(StandardCharsets.UTF_8).length
                                + path.toString().getBytes(StandardCharsets.UTF_8).length + sha256.length
                );

        buffer.putLong(index().id());
        buffer.putLong(site.id());
        buffer.putLong(epochHour.getEpochSecond());
        buffer.putLong(retention.getSeconds());
        buffer.put(namespace.name().getBytes(StandardCharsets.UTF_8));
        buffer.put(path.toString().getBytes(StandardCharsets.UTF_8));
        buffer.put(sha256);

        return buffer.array();
    }

    @Override
    public String toString() {
        return "MetadataImpl{" + "index=" + index + ", site=" + site + ", id=" + id + ", epochHour=" + epochHour
                + ", retention=" + retention + ", namespace=" + namespace + ", path=" + path + ", sha256="
                + Base64.getEncoder().encodeToString(sha256) + '}';
    }

}
