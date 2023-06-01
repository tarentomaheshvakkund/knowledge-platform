package org.sunbird.common;

import java.util.HashMap;
import java.util.Map;

import org.sunbird.common.exception.ServerException;

public class JWTUtil {
    private static String SEPARATOR = ".";
    private static String JWT_SECRET_STRING = Platform.config.hasPath("content_security_jwt_secret") ? 
        Platform.config.getString("content_security_jwt_secret"): "sunbird";
    
    public static String createHS256Token(Map<String, Object> claimsMap) {
        String token = "";
        JWTokenType tokenType = JWTokenType.HS256;
        try {
            Map<String, String> headerOptions = new HashMap<String, String>();
            String payLoad = createHeader(tokenType, headerOptions) + SEPARATOR + createClaimsMap(claimsMap);
            String signature = encodeToBase64Uri(
                    CryptoUtil.generateHMAC(payLoad, JWT_SECRET_STRING, tokenType.getAlgorithmName()));
            token = payLoad + SEPARATOR + signature;
        } catch (Exception e) {
            throw new ServerException("ERR_INVALID_HEADER_PARAM", "JWTUtil.createHS256Token :: Failed to create RS256 token. Err is : " + e.getMessage());
        }
        return token;
    }

    private static String createHeader(JWTokenType tokenType, Map<String, String> headerOptions) throws Exception {
        Map<String, String> headerData = new HashMap<>();
        if (headerOptions != null)
            headerData.putAll(headerOptions);
        headerData.put("alg", tokenType.getTokenType());
        headerData.put("typ", "JWT");
        return encodeToBase64Uri(JsonUtils.serialize(headerData).getBytes());
    }

    private static String createClaimsMap(Map<String, Object> claimsMap) throws Exception {
        Map<String, Object> payloadData = new HashMap<>();
        if (claimsMap != null && claimsMap.size() > 0) {
            payloadData.putAll(claimsMap);
        }
        return encodeToBase64Uri(JsonUtils.serialize(payloadData).getBytes());
    }

    private static String encodeToBase64Uri(byte[] data) {
        return Base64Util.encodeToString(data, 11);
    }
}
