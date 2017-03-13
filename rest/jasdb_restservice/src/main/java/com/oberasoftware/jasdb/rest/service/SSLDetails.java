package com.oberasoftware.jasdb.rest.service;

/**
 * @author Renze de Vries
 */
public class SSLDetails {
    private int sslPort;
    private String keystore;
    private String keystorePass;

    public SSLDetails(int sslPort, String keystore, String keystorePass) {
        this.sslPort = sslPort;
        this.keystore = keystore;
        this.keystorePass = keystorePass;
    }

    public int getSslPort() {
        return sslPort;
    }

    public String getKeystore() {
        return keystore;
    }

    public String getKeystorePass() {
        return keystorePass;
    }

    @Override
    public String toString() {
        return "SSLDetails{" +
                "sslPort=" + sslPort +
                ", keystore='" + keystore + '\'' +
                '}';
    }
}
