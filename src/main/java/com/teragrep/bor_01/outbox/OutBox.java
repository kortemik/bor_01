package com.teragrep.bor_01.outbox;

import com.teragrep.bor_01.metadata.Metadata;
import com.teragrep.bor_01.tree.MerkleRangeTree;

import java.util.List;

public interface OutBox {
    void objectFinalized(Metadata metadata);
    void objectStored(Metadata metadata);
    void metadataStored(Metadata metadata);

    List<Metadata> pendingObjectStore();
    List<Metadata> pendingMetadataStore();

    MerkleRangeTree tree();
}
