package com.oberasoftware.jasdb.acl;

import com.oberasoftware.jasdb.engine.metadata.Constants;
import nl.renarj.jasdb.api.SimpleEntity;

/**
 * @author Renze de Vries
 */
public class EncryptedGrants {
    private String objectName;
    private String encryptedData;
    private String salt;
    private String encryptionEngine;

    public EncryptedGrants(String objectName, String encryptedData, String salt, String encryptionEngine) {
        this.objectName = objectName;
        this.encryptedData = encryptedData;
        this.encryptionEngine = encryptionEngine;
        this.salt = salt;
    }

    public static EncryptedGrants fromEntity(SimpleEntity entity) {
        return new EncryptedGrants(entity.getValue(Constants.GRANT_OBJECT).toString(),
                entity.getValue(Constants.GRANTS).toString(),
                entity.getValue(Constants.SALT).toString(),
                entity.getValue(Constants.GRANT_ENCRYPTION).toString());
    }

    public static SimpleEntity toEntity(EncryptedGrants encryptedGrants) {
        SimpleEntity entity = new SimpleEntity();
        entity.addProperty(Constants.META_TYPE, GrantMetadataProvider.GRANT_TYPE);
        entity.addProperty(Constants.GRANT_OBJECT, encryptedGrants.getObjectName());
        entity.addProperty(Constants.GRANTS, encryptedGrants.getEncryptedData());
        entity.addProperty(Constants.SALT, encryptedGrants.getSalt());
        entity.addProperty(Constants.GRANT_ENCRYPTION, encryptedGrants.getEncryptionEngine());
        return entity;
    }

    public String getSalt() {
        return salt;
    }

    public String getEncryptionEngine() {
        return encryptionEngine;
    }

    public String getObjectName() {
        return objectName;
    }

    public String getEncryptedData() {
        return encryptedData;
    }
}
