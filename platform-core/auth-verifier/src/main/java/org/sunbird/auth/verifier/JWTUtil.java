package org.sunbird.auth.verifier;

import org.sunbird.common.JsonUtils;
import org.sunbird.telemetry.logger.TelemetryManager;

import java.util.HashMap;
import java.util.Map;

public class JWTUtil {
    private static String SEPARATOR = ".";

    public static String createRS256Token(Map<String, Object> claimsMap) {
        String token = "";
        JWTokenType tokenType = JWTokenType.RS256;
        try {
            KeyData keyData = KeyManager.getRandomKey();
            if(keyData != null) {
                Map<String, String> headerOptions = createHeaderOptions(keyData.getKeyId());
                String payLoad = createHeader(tokenType, headerOptions) + SEPARATOR + createClaimsMap(claimsMap);
                String signature = encodeToBase64Uri(CryptoUtil.generateRSASign(payLoad, keyData.getPrivateKey(), tokenType.getAlgorithmName()));
                token = payLoad + SEPARATOR + signature;
            } else {
                TelemetryManager.error("JWTUtil.createRS256Token :: KeyManager is not initialized properly.");
            }
        } catch (Exception e) {
            TelemetryManager.error("JWTUtil.createRS256Token :: Failed to create RS256 token. Exception: ", e);
        }
        return token;
    }

    private static String createHeader(JWTokenType tokenType, Map<String, String> headerOptions) throws Exception {
        Map<String, String> headerData = new HashMap<>();
        if (headerOptions != null)
            headerData.putAll(headerOptions);
        headerData.put("alg", tokenType.getTokenType());
        return encodeToBase64Uri(JsonUtils.serialize(headerData).getBytes());
    }

    private static Map<String, String> createHeaderOptions(String keyId) {
        Map<String, String> headers = new HashMap<>();
        headers.put("kid", keyId);
        return headers;
      }

    private static String createClaimsMap(Map<String, Object> claimsMap) throws Exception {
        Map<String, Object> payloadData = new HashMap<>();
        if(claimsMap != null && claimsMap.size() > 0) {
            payloadData.putAll(claimsMap);
        }
        return encodeToBase64Uri(JsonUtils.serialize(payloadData).getBytes());
    }

    private static String encodeToBase64Uri(byte[] data) {
        return Base64Util.encodeToString(data, 11);
    }
}
