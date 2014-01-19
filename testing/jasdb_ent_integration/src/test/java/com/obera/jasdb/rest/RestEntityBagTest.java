/*
 * The JASDB software and code is Copyright protected 2012 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2012 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package com.obera.jasdb.rest;

import nl.renarj.jasdb.SimpleBaseTest;
import nl.renarj.jasdb.core.SimpleKernel;
import nl.renarj.jasdb.core.platform.HomeLocatorUtil;
import nl.renarj.jasdb.service.EntityBagTest;

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
        System.setProperty(HomeLocatorUtil.JASDB_HOME, SimpleBaseTest.tmpDir.toString());
        super.setUp();
        SimpleKernel.initializeKernel();
    }
}
