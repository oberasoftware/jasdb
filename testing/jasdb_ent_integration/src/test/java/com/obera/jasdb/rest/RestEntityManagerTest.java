package com.obera.jasdb.rest;

import nl.renarj.jasdb.core.SimpleKernel;
import com.oberasoftware.jasdb.engine.HomeLocatorUtil;
import com.oberasoftware.jasdb.engine.EntityManagerTest;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

/**
 * @author Renze de Vries
 */
public class RestEntityManagerTest extends EntityManagerTest {
    public RestEntityManagerTest() {
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
