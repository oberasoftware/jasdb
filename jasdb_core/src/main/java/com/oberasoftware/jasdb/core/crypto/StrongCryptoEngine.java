package com.oberasoftware.jasdb.core.crypto;

import com.oberasoftware.jasdb.api.security.CryptoEngine;
import org.springframework.security.crypto.bcrypt.BCrypt;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.codec.Hex;
import org.springframework.security.crypto.encrypt.BytesEncryptor;
import org.springframework.security.crypto.encrypt.Encryptors;
import org.springframework.security.crypto.keygen.KeyGenerators;
import org.springframework.security.crypto.password.StandardPasswordEncoder;

import java.nio.charset.Charset;

/**
 * @author Renze de Vries
 */
public class StrongCryptoEngine implements CryptoEngine {
    private static final Charset UTF_CHARSET = Charset.forName("UTF8");
    public static final String STRONG_CRYPTO = "springStrongCrypto";

    @Override
    public String getDescriptor() {
        return STRONG_CRYPTO;
    }

    @Override
    public String encrypt(String salt, String password, String textToEncrypt) {

        BytesEncryptor bytesEncryptor = Encryptors.standard(password, new String(Hex.encode(salt.getBytes())));
        return new String(Hex.encode(bytesEncryptor.encrypt(textToEncrypt.getBytes(UTF_CHARSET))));
    }

    @Override
    public String decrypt(String salt, String password, String encrypted) {
        BytesEncryptor bytesEncryptor = Encryptors.standard(password, new String(Hex.encode(salt.getBytes())));

        return new String(bytesEncryptor.decrypt(Hex.decode(encrypted)), UTF_CHARSET);
    }

    @Override
    public String hash(String salt, String password) {
        return BCrypt.hashpw(password, salt);
    }

    @Override
    public String generateSalt() {
        return BCrypt.gensalt(10);
    }
}
