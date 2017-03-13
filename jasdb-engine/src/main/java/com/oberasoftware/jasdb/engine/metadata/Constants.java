package com.oberasoftware.jasdb.engine.metadata;

/**
 * @author Renze de Vries
 */
public interface Constants {
    /**
     * Instance Id constant
     */
    String INSTANCE = "instance";

    /**
     * Instance path property field constant
     */
    String INSTANCE_PATH = "path";

    /**
     * Name property field constant
     */
    String NAME = "name";

    /**
     * Indexes property name constant
     */
    String INDEXES = "indexes";
    String META_TYPE = "type";

    String INSTANCE_TYPE = "instance";
    String BAG_TYPE = "bag";

    /**
     * ACL Object separator
     */
    String OBJECT_SEPARATOR = "/";

    /**
     * User based constants
     */
    String USER_NAME = "username";
    String USER_PASSWORD_HASH = "hash";
    String USER_CONTENT_KEY = "contentkey";
    String SALT = "salt";
    String USER_ENGINE = "engine";
    String HOST = "host";

    /**
     * GrantObject based constants
     */
    String GRANT_OBJECT = "objectName";
    String GRANTS = "grants";
    String GRANT_USER = "grantUser";
    String GRANT_MODE = "grantMode";
    String GRANT_ENCRYPTION = "encryptionEngine";
}
