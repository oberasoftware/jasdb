/*
 * The JASDB software and code is Copyright protected 2012 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2012 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package com.oberasoftware.jasdb.integration.rest;

import com.oberasoftware.jasdb.test.EntityBagTest;

/**
 * User: renarj
 * Date: 5/11/12
 * Time: 6:19 PM
 */
public class RestEntityBagTest extends EntityBagTest {
    public RestEntityBagTest() {
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
