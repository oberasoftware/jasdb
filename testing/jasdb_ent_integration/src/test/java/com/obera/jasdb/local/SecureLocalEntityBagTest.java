/*
 * The JASDB software and code is Copyright protected 2012 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2012 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package com.obera.jasdb.local;

import nl.renarj.jasdb.service.EntityBagTest;

/**
 * @author Renze de Vries
 */
public class SecureLocalEntityBagTest extends EntityBagTest {
    public SecureLocalEntityBagTest() {
        super(new OverrideSecureSessionFactory());
    }

    @Override
    public void tearDown() throws Exception {
        System.setProperty("jasdb-config", "");
        super.tearDown();
    }

    @Override
    public void setUp() throws Exception {
        System.setProperty("jasdb-config", "jasdb-local-withsecurity.xml");
        super.setUp();
    }

}
