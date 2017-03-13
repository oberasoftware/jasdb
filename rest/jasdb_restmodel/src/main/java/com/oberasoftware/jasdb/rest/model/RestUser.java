package com.oberasoftware.jasdb.rest.model;

/**
 * @author Renze de Vries
 */
public class RestUser implements RestEntity {
    private String username;
    private String allowedHost;
    private String password;

    public RestUser(String username, String allowedHost, String password) {
        this.username = username;
        this.allowedHost = allowedHost;
        this.password = password;
    }

    public RestUser() {

    }

    public String getAllowedHost() {
        return allowedHost;
    }

    public void setAllowedHost(String allowedHost) {
        this.allowedHost = allowedHost;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
