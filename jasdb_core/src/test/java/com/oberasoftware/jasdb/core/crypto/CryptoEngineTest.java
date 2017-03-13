package com.oberasoftware.jasdb.core.crypto;

import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.security.CryptoEngine;
import org.junit.Test;

import java.nio.charset.Charset;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * @author Renze de Vries
 */
public class CryptoEngineTest {
    private static final String TEXT_TO_ENCRYPT = "This is My Super secret Text to Encrypt with some Text like this: 124567890";
    private static final Charset UTF_CHARSET = Charset.forName("UTF8");
    private static final String MY_PASS_WORD = "myPassWord112233";

    @Test
    public void testGetCryptoEngine() throws JasDBStorageException {
        CryptoEngine engine = CryptoFactory.getEngine();
        String salt = engine.generateSalt();
        String engineDescriptor = engine.getDescriptor();

        String encryptedText = engine.encrypt(salt, MY_PASS_WORD, TEXT_TO_ENCRYPT);
        assertThat(encryptedText, not(TEXT_TO_ENCRYPT));

        engine = CryptoFactory.getEngine(engineDescriptor);
        String decryptedText = engine.decrypt(salt, MY_PASS_WORD, encryptedText);
        assertThat(decryptedText, is(TEXT_TO_ENCRYPT));
    }
}
