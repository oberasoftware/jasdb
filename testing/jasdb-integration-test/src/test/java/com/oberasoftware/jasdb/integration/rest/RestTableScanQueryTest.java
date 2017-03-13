package com.oberasoftware.jasdb.integration.rest;

import com.oberasoftware.jasdb.test.TableScanQueryTest;

/**
 * @author Renze de Vries
 */
public class RestTableScanQueryTest extends TableScanQueryTest {
    public RestTableScanQueryTest() {
        super(new TestRestDBSessionFactory());
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        System.setProperty("jasdb-config", "");
    }

    @Override
    public void setUp() throws Exception {
        System.setProperty("jasdb-config", "jasdb-rest.xml");
        super.setUp();
    }

}
