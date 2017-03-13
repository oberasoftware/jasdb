package com.oberasoftware.jasdb.integration.rest;


import com.oberasoftware.jasdb.test.DBSessionTest;
import org.junit.After;
import org.junit.Before;

/**
 * @author Renze de Vries
 */
public class RestDBSessionTest extends DBSessionTest {
    public RestDBSessionTest() {
        super(new TestRestDBSessionFactory());
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        System.setProperty("jasdb-config", "");
    }

    @Before
    public void setUp() throws Exception {
        System.setProperty("jasdb-config", "jasdb-rest.xml");
        super.setUp();
    }
}
