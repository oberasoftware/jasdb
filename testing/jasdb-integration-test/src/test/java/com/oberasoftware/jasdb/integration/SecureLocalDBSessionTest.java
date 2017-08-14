package com.oberasoftware.jasdb.integration;


import com.oberasoftware.jasdb.test.DBSessionTest;
import org.junit.Ignore;

/**
 * @author Renze de Vries
 */
@Ignore
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
