package com.fdu.synserver.service;

import java.io.FileOutputStream;
import java.nio.file.Files;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;

import java.io.File;;
@Service
public class SignatureService {
    @org.springframework.beans.factory.annotation.Value("${my.custom.config.isServer}")
    private boolean isServer;
    @org.springframework.beans.factory.annotation.Value("${my.custom.config.keyPath}")
    private String keyPath;

    private final DBService dbService;

    // 缓存密钥对
    private Map<String, Object> keyCache = new ConcurrentHashMap<>();

    public SignatureService(DBService dbService) {
        this.dbService = dbService;
    }

    // 初始化密钥对
    @PostConstruct
    public void initKeyPairFromTables() {
        if (keyPath == null || keyPath.isEmpty()) {
            throw new IllegalStateException("Key path is not properly configured. Please check application.yml configuration.");
        }
        
        List<String> tables = dbService.getAllTableNames();
        for (String tableName : tables) {
            // 尝试从 keyPath 读取密钥
            File privateKeyFile = new File(keyPath + "/" + tableName + "_private.key");
            File publicKeyFile = new File(keyPath + "/" + tableName + "_public.key");
            System.out.println("privateKeyFile: " + privateKeyFile);
            System.out.println("publicKeyFile: " + publicKeyFile);
            if (isServer) {
                // Server模式：判断公钥是否存在，不存在报错，存在则缓存公钥
                if (!publicKeyFile.exists()) {
                    throw new IllegalStateException("Public key not found for table: " + tableName);
                } else {
                    try {
                        byte[] publicKeyBytes = Files.readAllBytes(publicKeyFile.toPath());
                        KeyFactory keyFactory = KeyFactory.getInstance("EC");
                        PublicKey publicKey = keyFactory.generatePublic(new X509EncodedKeySpec(publicKeyBytes));
                        keyCache.put(tableName, publicKey);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            } else {
                // Client模式：判断私钥是否存在，不存在就创建公私钥对，存在则缓存私钥
                if (!privateKeyFile.exists()) {
                    generateKeyPair(tableName);
                } else {
                    try {
                        byte[] privateKeyBytes = Files.readAllBytes(privateKeyFile.toPath());
                        KeyFactory keyFactory = KeyFactory.getInstance("EC");
                        PrivateKey privateKey = keyFactory.generatePrivate(new PKCS8EncodedKeySpec(privateKeyBytes));
                        keyCache.put(tableName, privateKey);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    // 生成密钥对并保存到文件
    public void generateKeyPair(String tableName) {
        try {
            KeyPairGenerator keyGen = KeyPairGenerator.getInstance("EC");
            keyGen.initialize(256); // 使用常见的256位椭圆曲线
            KeyPair keyPair = keyGen.generateKeyPair();

            // 缓存需要的密钥
            if (isServer) {
                keyCache.put(tableName, keyPair.getPublic());
            } else {
                keyCache.put(tableName, keyPair.getPrivate());
            }

            // 获取私钥和公钥
            PrivateKey privateKey = keyPair.getPrivate();
            PublicKey publicKey = keyPair.getPublic();

            // 保存私钥到文件
            File privateKeyFile = new File(keyPath + "/" + tableName + "_private.key");
            try (FileOutputStream fos = new FileOutputStream(privateKeyFile)) {
                fos.write(privateKey.getEncoded());
            }

            // 保存公钥到文件
            File publicKeyFile = new File(keyPath + "/" + tableName + "_public.key");
            try (FileOutputStream fos = new FileOutputStream(publicKeyFile)) {
                fos.write(publicKey.getEncoded());
            }
        } catch (Exception e) {
            // 打印异常日志
            e.printStackTrace();
        }
    }

    // 获取缓存的密钥（私钥或公钥）
    public Object getKey(String tableName) {
        return keyCache.get(tableName);
    }

    // 使用私钥对数据进行签名
    public byte[] signData(String tableName, byte[] data) {
        try {
            PrivateKey privateKey = (PrivateKey) keyCache.get(tableName);
            if (privateKey == null) {
                throw new IllegalStateException("Private key not found for table: " + tableName);
            }
            Signature signature = Signature.getInstance("SHA256withECDSA");
            signature.initSign(privateKey);
            signature.update(data);
            return signature.sign();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    // 使用公钥验证签名
    public boolean verifySignature(String tableName, byte[] data, byte[] signatureBytes) {
        try {
            PublicKey publicKey = (PublicKey) keyCache.get(tableName);
            if (publicKey == null) {
                throw new IllegalStateException("Public key not found for table: " + tableName);
            }
            Signature signature = Signature.getInstance("SHA256withECDSA");
            signature.initVerify(publicKey);
            signature.update(data);
            return signature.verify(signatureBytes);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }
}
