package com.oberasoftware.jasdb.rest.model.streaming;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.oberasoftware.jasdb.rest.model.RestEntity;

/**
 * @author Renze de Vries
 */
public class OauthToken implements RestEntity {
    private String oauthToken;
    private String sessionId;
    private long expiration;
    private String tokenType;
    private String grant;
    private String message;

    public OauthToken() {

    }

    @JsonProperty(value = "grant")
    public String getGrant() {
        return grant;
    }

    public void setGrant(String grant) {
        this.grant = grant;
    }

    @JsonProperty(value = "message")
    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    @JsonProperty(value = "access_token")
    public String getOauthToken() {
        return oauthToken;
    }

    public void setOauthToken(String oauthToken) {
        this.oauthToken = oauthToken;
    }

    @JsonProperty(value = "sessionid")
    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    @JsonProperty(value = "expires_in")
    public long getExpiration() {
        return expiration;
    }

    public void setExpiration(long expiration) {
        this.expiration = expiration;
    }

    @JsonProperty(value = "token_type")
    public String getTokenType() {
        return tokenType;
    }

    public void setTokenType(String tokenType) {
        this.tokenType = tokenType;
    }
}
