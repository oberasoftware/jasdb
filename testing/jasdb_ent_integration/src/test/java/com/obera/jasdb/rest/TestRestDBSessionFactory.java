/*
 * The JASDB software and code is Copyright protected 2012 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2012 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package com.obera.jasdb.rest;

import nl.renarj.jasdb.api.DBSession;
import nl.renarj.jasdb.api.context.Credentials;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.rest.RestBaseTest;
import nl.renarj.jasdb.rest.client.RestDBSessionFactory;

/**
 * User: renarj
 * Date: 4/28/12
 * Time: 6:34 PM
 */
public class TestRestDBSessionFactory extends RestDBSessionFactory {
    private Credentials credentials;

    public TestRestDBSessionFactory() {
        setHostname("localhost");
        setPort(RestBaseTest.DEFAULT_PORT);
        setInstanceId(RestBaseTest.INSTANCE_ID);
    }

    public TestRestDBSessionFactory(Credentials credentials) {
        super(false);
        this.credentials = credentials;
        setHostname("localhost");
        setPort(RestBaseTest.DEFAULT_SSL_PORT);
        setInstanceId(RestBaseTest.INSTANCE_ID);
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
