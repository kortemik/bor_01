package com.teragrep.bor_01.tree;

import com.goterl.lazysodium.interfaces.Ristretto255;

public interface MerkleRangeTree {
    Ristretto255.RistrettoPoint root();
    MerkleRangeTree leafAdd(byte[] sha512, Ristretto255.RistrettoPoint point);
    MerkleRangeTree leafRemove(byte[] sha512, Ristretto255.RistrettoPoint point);
}
