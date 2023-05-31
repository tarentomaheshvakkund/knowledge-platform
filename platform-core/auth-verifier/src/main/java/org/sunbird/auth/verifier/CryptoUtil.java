package org.sunbird.auth.verifier;

import java.nio.charset.Charset;
import java.security.*;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.sunbird.telemetry.logger.TelemetryManager;


public class CryptoUtil {
    private static final Charset US_ASCII = Charset.forName("US-ASCII");

    public static boolean verifyRSASign(String payLoad, byte[] signature, PublicKey key, String algorithm) {
        Signature sign;
        try {
            sign = Signature.getInstance(algorithm);
            sign.initVerify(key);
            sign.update(payLoad.getBytes(US_ASCII));
            return sign.verify(signature);
        } catch (NoSuchAlgorithmException e) {
            return false;
        } catch (InvalidKeyException e){
            return false;
        } catch (SignatureException e){
            return false;
        }
    }

    public static byte[] generateHMAC(String payLoad, String secretKey, String algorithm) {
        Mac mac;
        byte[] signature;
        try {
            mac = Mac.getInstance(algorithm);
            mac.init(new SecretKeySpec(secretKey.getBytes(), algorithm));
            signature = mac.doFinal(payLoad.getBytes(US_ASCII));
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            TelemetryManager.error("CryptoUtil:generateHMAC :: failed to generate signature. Exception: ", e);
            return null;
        }
        return signature;
    }

    public static byte[] generateRSASign(String payLoad, PrivateKey key, String algorithm) {
        Signature sign;
        byte[] signature;
        try {
            sign = Signature.getInstance(algorithm);
            sign.initSign(key);
            sign.update(payLoad.getBytes(US_ASCII));
            signature = sign.sign();
        } catch (NoSuchAlgorithmException | InvalidKeyException | SignatureException e) {
            TelemetryManager.error("CryptoUtil:generateRSASign :: failed to generate signature. Exception: ", e);
            return null;
        }
        return signature;
    }
}
