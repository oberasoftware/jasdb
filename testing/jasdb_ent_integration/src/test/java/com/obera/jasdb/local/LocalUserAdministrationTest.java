package com.obera.jasdb.local;

import com.obera.jasdb.UserAdministrationTest;

/**
 * @author Renze de Vries
 */
public class LocalUserAdministrationTest extends UserAdministrationTest {
    public LocalUserAdministrationTest() {
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
