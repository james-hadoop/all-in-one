package com.james.demo.encryption;

import java.security.Key;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESKeySpec;

import org.apache.commons.codec.binary.Base64;

public class SymmetricEncryption {
    public static final String KEY_ALGORITHM = "DES";

    public static final String CIPHER_ALGORITHM = "DES/ECB/PKCS5Padding";

    private static Key toKey(byte[] key) throws Exception {
        DESKeySpec dks = new DESKeySpec(key);

        SecretKeyFactory keyFactory = SecretKeyFactory.getInstance(KEY_ALGORITHM);

        SecretKey secretKey = keyFactory.generateSecret(dks);

        return secretKey;
    }

    public static byte[] decrypt(byte[] data, byte[] key) throws Exception {
        Key k = toKey(key);

        Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);

        cipher.init(Cipher.DECRYPT_MODE, k);

        return cipher.doFinal(data);
    }

    public static String decrypt(String data, byte[] key) throws Exception {
        Key k = toKey(key);

        Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);

        cipher.init(Cipher.DECRYPT_MODE, k);

        return new String(cipher.doFinal(Base64.decodeBase64(data)));
    }

    public static byte[] encrypt(byte[] data, byte[] key) throws Exception {
        Key k = toKey(key);

        Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);

        cipher.init(Cipher.ENCRYPT_MODE, k);

        return cipher.doFinal(data);
    }

    public static String encrypt(String data, byte[] key) throws Exception {
        Key k = toKey(key);

        Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);

        cipher.init(Cipher.ENCRYPT_MODE, k);

        return Base64.encodeBase64String(cipher.doFinal(data.getBytes()));
    }

    public static byte[] initKey() throws Exception {
        KeyGenerator kg = KeyGenerator.getInstance(KEY_ALGORITHM);

        kg.init(56);

        SecretKey secretKey = kg.generateKey();

        return secretKey.getEncoded();
    }

    public static void main(String[] args) throws Exception {
        byte[] rawKey = initKey();
        byte[] data = "data".getBytes();

        byte[] byteEncryptData = encrypt(data, rawKey);
        String encryptData = Base64.encodeBase64String(byteEncryptData);
        byte[] byteEncryptDataVerify = Base64.decodeBase64(encryptData);

        System.out.println("encryptData=" + encryptData);

        byte[] byteDecryptData = decrypt(byteEncryptData, rawKey);
        String decryptData = new String(byteDecryptData);
        System.out.println("decryptData=" + decryptData);

        System.out.println("--------------------------------------------------------");

        String data2 = "data2";
        encryptData = encrypt(data2, rawKey);
        decryptData = decrypt(encryptData, rawKey);

        System.out.println("encryptData=" + encryptData);
        System.out.println("decryptData=" + decryptData);

        System.out.println("--------------------------------------------------------");

        byte[] keySeed = "key_bytes".getBytes();

        encryptData = encrypt(data2, keySeed);
        decryptData = decrypt(encryptData, keySeed);

        System.out.println("encryptData=" + encryptData);
        System.out.println("decryptData=" + decryptData);
    }
}
