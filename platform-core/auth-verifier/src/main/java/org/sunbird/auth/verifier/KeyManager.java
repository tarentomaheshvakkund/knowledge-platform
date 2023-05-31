package org.sunbird.auth.verifier;

import java.util.Map;
import org.sunbird.telemetry.logger.TelemetryManager;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.PrivateKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class KeyManager {
    public static final String ACCESS_TOKEN_PUBLICKEY_BASEPATH = "accesstoken_privatekey_basepath";

    private static PropertiesCache propertiesCache = PropertiesCache.getInstance();
    private static Map<String, KeyData> keyMap = new HashMap<String, KeyData>();

    public static void init() {
        TelemetryManager.info("KeyManager:init :: Starting initialization...");
        String basePath = propertiesCache.getProperty(ACCESS_TOKEN_PUBLICKEY_BASEPATH);
        try (Stream<Path> walk = Files.walk(Paths.get(basePath))) {
            List<String> result =
                    walk.filter(Files::isRegularFile).map(x -> x.toString()).collect(Collectors.toList());
            result.forEach(
                    file -> {
                        try {
                            StringBuilder contentBuilder = new StringBuilder();
                            Path path = Paths.get(file);
                            Files.lines(path, StandardCharsets.UTF_8)
                                    .forEach(
                                            x -> {
                                                contentBuilder.append(x);
                                            });
                            KeyData keyData =
                                    new KeyData(
                                            path.getFileName().toString(), null, loadPrivateKey(contentBuilder.toString()));
                            keyMap.put(path.getFileName().toString(), keyData);
                        } catch (Exception e) {
                            TelemetryManager.error("KeyManager:init: exception in reading public keys ", e);
                        }
                    });
        } catch (Exception e) {
            TelemetryManager.error("KeyManager:init: exception in loading publickeys ", e);
        }
    }

    public static KeyData getRandomKey() {
        if (keyMap.size() == 0) {
            init();
        }
        if (keyMap.size() > 0) {
            Random random = new Random();
            List<String> keys = new ArrayList<String>(keyMap.keySet());
            String randomKey = keys.get(random.nextInt(keys.size()));
            return keyMap.get(randomKey);
        }
        return null;
    }

    public static PublicKey loadPublicKey(String key) throws Exception {
        String publicKey = new String(key.getBytes(), StandardCharsets.UTF_8);
        publicKey = publicKey.replaceAll("(-+BEGIN PUBLIC KEY-+)", "");
        publicKey = publicKey.replaceAll("(-+END PUBLIC KEY-+)", "");
        publicKey = publicKey.replaceAll("[\\r\\n]+", "");
        byte[] keyBytes = Base64Util.decode(publicKey.getBytes("UTF-8"), Base64Util.DEFAULT);

        X509EncodedKeySpec X509publicKey = new X509EncodedKeySpec(keyBytes);
        KeyFactory kf = KeyFactory.getInstance("RSA");
        return kf.generatePublic(X509publicKey);
    }

    public static PrivateKey loadPrivateKey(String key) throws Exception {
        String privateKey = new String(key.getBytes(), StandardCharsets.UTF_8);
        privateKey = privateKey.replaceAll("(-+BEGIN RSA PRIVATE KEY-+)",                                       "");
        privateKey = privateKey.replaceAll("(-+END RSA PRIVATE KEY-+)", "");
        privateKey = privateKey.replaceAll("(-+BEGIN PRIVATE KEY-+)", "");
        privateKey = privateKey.replaceAll("(-+END PRIVATE KEY-+)", "");
        privateKey = privateKey.replaceAll("[\\r\\n]+", "");
        byte[] keyBytes = Base64Util.decode(privateKey.getBytes("UTF-8"), Base64Util.DEFAULT);

        // generate private key
        PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(keyBytes);
        KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        return keyFactory.generatePrivate(spec);
    }
}
