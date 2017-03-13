package com.obera.jasdb.local;

import com.oberasoftware.jasdb.engine.DBSessionTest;

/**
 * @author Renze de Vries
 */
public class SecureLocalDBSessionTest extends DBSessionTest {
    public SecureLocalDBSessionTest() {
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
