package nl.renarj.jasdb.core.crypto;

import nl.renarj.core.utilities.StringUtils;
import nl.renarj.jasdb.core.exceptions.JasDBSecurityException;
import org.springframework.security.crypto.codec.Hex;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.Charset;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;

/**
 * @author Renze de Vries
 */
public class BasicCryptoEngine implements CryptoEngine {
	private static final String STANDARD_EMPTY_PASS = " ";
    public static final String BASIC_CRYPTO = "basic_128";

    private static final Charset UTF8 = Charset.forName("UTF8");
    private static final int IV_SIZE = 16;
    private static final int SALT_SIZE = 32;

    private SecureRandom secureRandom;

    public BasicCryptoEngine() {
        secureRandom = new SecureRandom();

    }

    @Override
    public String getDescriptor() {
        return BASIC_CRYPTO;
    }
    
    private String getStandardizedPassword(String password) {
    	if(StringUtils.stringEmpty(password)) {
    		return STANDARD_EMPTY_PASS;
    	} else {
    		return password;
    	}
    }

    @Override
    public String encrypt(String salt, String password, String textToEncrypt) throws JasDBSecurityException {
    	String standardPass = getStandardizedPassword(password);
        PBEKeySpec keySpec = new PBEKeySpec(standardPass.toCharArray(), Hex.decode(salt), 1024, 128);

        try {
            byte[] iv = new byte[IV_SIZE];
            secureRandom.nextBytes(iv);

            SecretKey secretKey = new SecretKeySpec(SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1").generateSecret(keySpec).getEncoded(), "AES");
            Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
            cipher.init(Cipher.ENCRYPT_MODE, secretKey, new IvParameterSpec(iv));
            byte[] encrypted = cipher.doFinal(textToEncrypt.getBytes(UTF8));


            return new String(Hex.encode(appendArrays(iv, encrypted)));
        } catch (InvalidKeySpecException e) {
            throw new JasDBSecurityException("Unable to encrypt", e);
        } catch (NoSuchAlgorithmException e) {
            throw new JasDBSecurityException("Unable to encrypt", e);
        } catch (NoSuchPaddingException e) {
            throw new JasDBSecurityException("Unable to encrypt", e);
        } catch (InvalidAlgorithmParameterException e) {
            throw new JasDBSecurityException("Unable to encrypt", e);
        } catch (InvalidKeyException e) {
            throw new JasDBSecurityException("Unable to encrypt", e);
        } catch (BadPaddingException e) {
            throw new JasDBSecurityException("Unable to encrypt", e);
        } catch (IllegalBlockSizeException e) {
            throw new JasDBSecurityException("Unable to encrypt", e);
        }
    }

    private byte[] appendArrays(byte[] firstArray, byte[] secondArray) {
        final byte[] result = new byte[firstArray.length + secondArray.length];

        System.arraycopy(firstArray, 0, result, 0, firstArray.length);
        System.arraycopy(secondArray, 0, result, firstArray.length, secondArray.length);

        return result;
    }

    @Override
    public String decrypt(String salt, String password, String encryptedText) throws JasDBSecurityException {
        String standardPass = getStandardizedPassword(password);
    	byte[] encryptedData = Hex.decode(encryptedText);
        byte[] iv = new byte[IV_SIZE];
        System.arraycopy(encryptedData, 0, iv, 0, IV_SIZE);
        byte[] encrypted = new byte[encryptedData.length - IV_SIZE];
        System.arraycopy(encryptedData, IV_SIZE, encrypted, 0, encryptedData.length - IV_SIZE);

        PBEKeySpec keySpec = new PBEKeySpec(standardPass.toCharArray(), Hex.decode(salt), 1024, 128);
        try {
            SecretKeyFactory skf = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
            SecretKey sk = skf.generateSecret(keySpec);
            SecretKey secretKey = new SecretKeySpec(sk.getEncoded(), "AES");
            Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
            cipher.init(Cipher.DECRYPT_MODE, secretKey, new IvParameterSpec(iv));

            return new String(cipher.doFinal(encrypted), UTF8);
        } catch(NoSuchAlgorithmException e) {
            throw new JasDBSecurityException("Unable to decrypt", e);
        } catch (InvalidKeySpecException e) {
            throw new JasDBSecurityException("Unable to decrypt", e);
        } catch (InvalidKeyException e) {
            throw new JasDBSecurityException("Unable to decrypt", e);
        } catch (InvalidAlgorithmParameterException e) {
            throw new JasDBSecurityException("Unable to decrypt", e);
        } catch (NoSuchPaddingException e) {
            throw new JasDBSecurityException("Unable to decrypt", e);
        } catch (BadPaddingException e) {
            throw new JasDBSecurityException("Unable to decrypt", e);
        } catch (IllegalBlockSizeException e) {
            throw new JasDBSecurityException("Unable to decrypt", e);
        }
    }

    @Override
    public String hash(String salt, String password) throws JasDBSecurityException {
        try {
            String salted = salt + password;
            MessageDigest messageDigest = MessageDigest.getInstance("SHA-1");
            byte[] digest = messageDigest.digest(salted.getBytes(UTF8));
            return new String(Hex.encode(digest));
        } catch(NoSuchAlgorithmException e) {
            throw new JasDBSecurityException("Unable to hash", e);
        }
    }

    @Override
    public String generateSalt() {
        byte[] salt = new byte[SALT_SIZE];
        secureRandom.nextBytes(salt);

        return new String(Hex.encode(salt));
    }
}
