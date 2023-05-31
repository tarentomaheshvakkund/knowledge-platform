package org.sunbird.auth.verifier;

public enum JWTokenType {
    HS256("HS256", "HmacSHA256"),
    RS256("RS256", "SHA256withRSA");

    private String algorithmName;
    private String tokenType;

    JWTokenType(String tokenType, String algorithmName) {
        this.algorithmName = algorithmName;
        this.tokenType = tokenType;
    }

    public String getAlgorithmName() {
        return algorithmName;
    }

    public String getTokenType() {
        return tokenType;
    }
}
