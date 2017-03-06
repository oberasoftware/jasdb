/*
 * The JASDB software and code is Copyright protected 2012 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2012 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package com.obera.jasdb.rest;

import nl.renarj.jasdb.core.SimpleKernel;
import com.oberasoftware.jasdb.engine.HomeLocatorUtil;
import com.oberasoftware.jasdb.engine.EntityQueryTest;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

/**
 * @author Renze de Vries
 */
public class RestQueryTest extends EntityQueryTest {
    public RestQueryTest() {
        super(new TestRestDBSessionFactory());
    }

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        System.setProperty("jasdb-config", "");
    }

    @Override
    public void setUp() throws Exception {
        System.setProperty("jasdb-config", "jasdb-rest.xml");
        System.setProperty(HomeLocatorUtil.JASDB_HOME, temporaryFolder.newFolder().toString());
        SimpleKernel.initializeKernel();

        super.setUp();
    }
}
