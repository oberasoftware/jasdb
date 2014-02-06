package com.obera.service.acl;

import nl.renarj.jasdb.api.context.Credentials;

/**
 * @author Renze de Vries
 */
public class BasicCredentials implements Credentials {
    private String username;
    private String sourceHost;
    private String password;

    public BasicCredentials(String username, String sourceHost, String password) {
        this.username = username;
        this.sourceHost = sourceHost;
        this.password = password;
    }

    public BasicCredentials(String username, String password) {
        this.username = username;
        this.password = password;
    }

    @Override
    public String getSourceHost() {
        return sourceHost;
    }

    @Override
    public String getUsername() {
        return username;
    }

    @Override
    public String getPassword() {
        return password;
    }
}
