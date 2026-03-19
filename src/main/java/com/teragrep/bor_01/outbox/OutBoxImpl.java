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
package com.teragrep.bor_01.outbox;

import com.goterl.lazysodium.LazySodiumJava;
import com.goterl.lazysodium.exceptions.SodiumException;
import com.goterl.lazysodium.interfaces.Ristretto255;
import com.teragrep.bor_01.id.Id;
import com.teragrep.bor_01.metadata.Index;
import com.teragrep.bor_01.metadata.Metadata;
import com.teragrep.bor_01.tree.MerkeTreeStub;
import com.teragrep.bor_01.tree.MerkleTree;
import com.teragrep.bor_01.tree.MerkleTreeImpl;

import java.security.NoSuchAlgorithmException;
import java.util.*;

public class OutBoxImpl implements OutBox {

    // todo predefine indexes and their trees?
    private final LazySodiumJava lazySodiumJava;
    private final Map<Id, Metadata> finalizedObjects;
    private final Map<Id, Metadata> storedObjects;
    private final Map<Index, MerkleTree> merkleRangeTrees;

    public OutBoxImpl(final LazySodiumJava lazySodiumJava) {
        this(lazySodiumJava, new HashMap<>(), new HashMap<>(), new HashMap<>());
    }

    private OutBoxImpl(
            final LazySodiumJava lazySodiumJava,
            final HashMap<Id, Metadata> finalizedObjects,
            final HashMap<Id, Metadata> storedObjects,
            final HashMap<Index, MerkleTree> merkleRangeTrees
    ) {
        this.lazySodiumJava = lazySodiumJava;
        this.finalizedObjects = finalizedObjects;
        this.storedObjects = storedObjects;
        this.merkleRangeTrees = merkleRangeTrees;

    }

    @Override
    public synchronized void objectFinalized(final Metadata metadata) {
        if (storedObjects.containsKey(metadata.id())) {
            throw new IllegalStateException("Metadata already stored");
        }
        if (finalizedObjects.containsKey(metadata.id())) {
            throw new IllegalStateException("Metadata already finalized");
        }
        finalizedObjects.put(metadata.id(), metadata);
    }

    @Override
    public synchronized void objectStored(final Metadata metadata) {
        if (!finalizedObjects.containsKey(metadata.id())) {
            throw new IllegalStateException("Metadata not finalized");
        }
        if (storedObjects.containsKey(metadata.id())) {
            throw new IllegalStateException("Metadata already stored");
        }
        // should tree be checked for presence as well?
        Metadata metadata1 = finalizedObjects.remove(metadata.id());
        storedObjects.put(metadata.id(), metadata1);

    }

    @Override
    public synchronized void metadataStored(final Metadata metadata) throws SodiumException, NoSuchAlgorithmException {
        if (finalizedObjects.containsKey(metadata.id())) {
            throw new IllegalStateException("Metadata still finalized");
        }
        if (!storedObjects.containsKey(metadata.id())) {
            throw new IllegalStateException("Metadata not stored");
        }
        Metadata metadata1 = storedObjects.remove(metadata.id());

        if (!merkleRangeTrees.containsKey(metadata1.index())) {
            Ristretto255.RistrettoPoint basePoint = Ristretto255.RistrettoPoint.base(lazySodiumJava);
            merkleRangeTrees.put(metadata1.index(), new MerkleTreeImpl(lazySodiumJava, basePoint));
        }

        merkleRangeTrees.get(metadata1.index()).addMetadataPoint(metadata1.epochHour(), metadata1.point());
    }

    @Override
    public synchronized List<Metadata> pendingObjectStore() {
        return new ArrayList<>(finalizedObjects.values());
    }

    @Override
    public synchronized List<Metadata> pendingMetadataStore() {
        return new ArrayList<>(storedObjects.values());
    }

    @Override
    public synchronized MerkleTree tree(Index index) {
        final MerkleTree result;
        if (!merkleRangeTrees.containsKey(index)) {
            result = merkleRangeTrees.get(index);
        }
        else {
            result = new MerkeTreeStub();
        }
        return result;
    }

    @Override
    public synchronized Map<Index, MerkleTree> trees() {
        return new HashMap<>(merkleRangeTrees);
    }

    @Override
    public synchronized void addIndex(final Index index) throws SodiumException {
        Ristretto255.RistrettoPoint basePoint = Ristretto255.RistrettoPoint.base(lazySodiumJava);
        merkleRangeTrees.put(index, new MerkleTreeImpl(lazySodiumJava, basePoint));
    }

}
