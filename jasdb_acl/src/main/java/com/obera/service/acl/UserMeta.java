package com.obera.service.acl;

import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.api.metadata.User;
import nl.renarj.jasdb.service.metadata.Constants;

/**
 * @author Renze de Vries
 */
public final class UserMeta implements User {


    private final String username;
    private final String contentKey;
    private final String host;
    private final String salt;
    private final String hash;
    private final String engine;

    public UserMeta(String userName, String host, String contentKey, String salt, String hash, String engine) {
        this.username = userName;
        this.host = host;
        this.contentKey = contentKey;
        this.salt = salt;
        this.hash = hash;
        this.engine = engine;
    }

    public static User fromEntity(SimpleEntity entity) {
        String userName = entity.getValue(Constants.USER_NAME).toString();
        String host = entity.getValue(Constants.HOST).toString();
        String contentKey = entity.getValue(Constants.USER_CONTENT_KEY).toString();
        String salt = entity.getValue(Constants.SALT).toString();
        String hash = entity.getValue(Constants.USER_PASSWORD_HASH).toString();
        String engine = entity.getValue(Constants.USER_ENGINE).toString();
        return new UserMeta(userName, host, contentKey, salt, hash, engine);
    }

    public static SimpleEntity toEntity(User user) {
        SimpleEntity entity = new SimpleEntity();
        entity.addProperty(Constants.USER_NAME, user.getUsername());
        entity.addProperty(Constants.HOST, user.getHost());
        entity.addProperty(Constants.USER_CONTENT_KEY, user.getEncryptedContentKey());
        entity.addProperty(Constants.SALT, user.getPasswordSalt());
        entity.addProperty(Constants.USER_PASSWORD_HASH, user.getPasswordHash());
        entity.addProperty(Constants.USER_ENGINE, user.getEncryptionEngine());
        entity.addProperty(Constants.META_TYPE, UserMetadataProvider.USERMETA_TYPE);
        return entity;
    }

    @Override
    public String getHost() {
        return host;
    }

    @Override
    public String getEncryptionEngine() {
        return engine;
    }

    @Override
    public String getUsername() {
        return username;
    }

    @Override
    public String getEncryptedContentKey() {
        return contentKey;
    }

    @Override
    public String getPasswordSalt() {
        return salt;
    }

    @Override
    public String getPasswordHash() {
        return hash;
    }

    @Override
    public String toString() {
        return "UserMeta{" +
                "username='" + username + '\'' +
                ", host='" + host + '\'' +
                ", engine='" + engine + '\'' +
                '}';
    }
}
