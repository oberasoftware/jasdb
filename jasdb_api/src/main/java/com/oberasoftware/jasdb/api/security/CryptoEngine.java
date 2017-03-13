package com.oberasoftware.jasdb.api.security;

import com.oberasoftware.jasdb.api.exceptions.JasDBSecurityException;

/**
 * @author Renze de Vries
 */
public interface CryptoEngine {
    String getDescriptor();

    String encrypt(String salt, String password, String text) throws JasDBSecurityException;

    String decrypt(String salt, String password, String encrypted) throws JasDBSecurityException;

    String generateSalt() throws JasDBSecurityException;

    String hash(String salt, String password) throws JasDBSecurityException;
}
