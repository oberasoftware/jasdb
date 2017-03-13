package com.oberasoftware.jasdb.api.model;

public interface User {
    String getUsername();

    String getHost();

    String getEncryptionEngine();

    String getEncryptedContentKey();

    String getPasswordSalt();

    String getPasswordHash();
}
