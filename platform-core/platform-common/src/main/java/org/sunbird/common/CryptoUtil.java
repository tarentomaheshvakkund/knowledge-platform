package org.sunbird.common;

import java.nio.charset.Charset;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

public class CryptoUtil {
    private static final Charset US_ASCII = Charset.forName("US-ASCII");

    public static byte[] generateHMAC(String payLoad, String secretKey, String algorithm) {
        Mac mac;
        byte[] signature;
        try {
            mac = Mac.getInstance(algorithm);
            mac.init(new SecretKeySpec(secretKey.getBytes(), algorithm));
            signature = mac.doFinal(payLoad.getBytes(US_ASCII));
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            return null;
        }
        return signature;
    }
}
