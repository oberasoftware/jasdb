package com.obera.jasdb.local;

import nl.renarj.jasdb.service.EntityQueryTest;

/**
 * @author Renze de Vries
 */
public class SecureLocalQueryTest extends EntityQueryTest {
    public SecureLocalQueryTest() {
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
