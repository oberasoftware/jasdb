/*
 * The JASDB software and code is Copyright protected 2012 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2012 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package com.oberasoftware.jasdb.integration.rest;

import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.security.Credentials;
import com.oberasoftware.jasdb.api.session.DBSession;
import com.oberasoftware.jasdb.rest.client.RestDBSessionFactory;

/**
 * User: renarj
 * Date: 4/28/12
 * Time: 6:34 PM
 */
public class TestRestDBSessionFactory extends RestDBSessionFactory {
    private Credentials credentials;

    public static String INSTANCE_ID = "default";
    public static String BAG_NAME = "bag0";
    public static int DEFAULT_PORT = 7050;


    public TestRestDBSessionFactory() {
        setHostname("localhost");
        setPort(DEFAULT_PORT);
        setInstanceId(INSTANCE_ID);
    }

    public TestRestDBSessionFactory(Credentials credentials) {
        super(false);
        this.credentials = credentials;
        setHostname("localhost");
        setPort(DEFAULT_PORT);
        setInstanceId(INSTANCE_ID);
    }

    @Override
    public DBSession createSession() throws JasDBStorageException {
        if(credentials != null) {
            return super.createSession(credentials);
        } else {
            return super.createSession();
        }
    }

    @Override
    public DBSession createSession(String instance) throws JasDBStorageException {
        if(credentials != null) {
            return super.createSession(instance, credentials);
        } else {
            return super.createSession(instance);
        }
    }
}
