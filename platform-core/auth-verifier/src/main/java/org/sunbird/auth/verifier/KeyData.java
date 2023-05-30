package org.sunbird.auth.verifier;

import java.security.PrivateKey;
import java.security.PublicKey;

public class KeyData {
    private String keyId;
    private PrivateKey privateKey;
    private PublicKey publicKey;

    public KeyData(String keyId, PublicKey publicKey, PrivateKey privateKey) {
        this.keyId = keyId;
        this.publicKey = publicKey;
        this.privateKey = privateKey;
    }

    public String getKeyId() {
        return keyId;
    }

    public void setKeyId(String keyId) {
        this.keyId = keyId;
    }

    public PublicKey getPublicKey() {
        return publicKey;
    }

    public void setPublicKey(PublicKey publicKey) {
        this.publicKey = publicKey;
    }

    public PrivateKey getPrivateKey() {
        return privateKey;
    }

    public void setPrivateKey(PrivateKey privateKey) {
        this.privateKey = privateKey;
    }
}
