package nl.renarj.jasdb.api.metadata;

public interface User {
    String getUsername();

    String getHost();

    String getEncryptionEngine();

    String getEncryptedContentKey();

    String getPasswordSalt();

    String getPasswordHash();
}
