package com.teragrep.bor_01;

import com.goterl.lazysodium.LazySodiumJava;
import com.goterl.lazysodium.SodiumJava;
import com.goterl.lazysodium.exceptions.SodiumException;
import com.goterl.lazysodium.interfaces.Ristretto255;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
}
