package com.oberasoftware.jasdb.service.local;

import com.oberasoftware.jasdb.engine.HomeLocatorUtil;
import com.oberasoftware.jasdb.service.JasDBMain;
import com.oberasoftware.jasdb.api.session.DBSession;
import com.oberasoftware.jasdb.api.exceptions.JasDBException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * @author Renze de Vries
 */
public class LocalDBSessionFactoryTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void before() throws Exception {
        System.setProperty(HomeLocatorUtil.JASDB_HOME, temporaryFolder.newFolder().toString());
    }

    @After
    public void tearDown() throws Exception {
        JasDBMain.shutdown();
    }

    @Test
    public void testCreateDefaultSession() throws JasDBException {
        LocalDBSessionFactory localDBSessionFactory = new LocalDBSessionFactory();
        DBSession dbSession = localDBSessionFactory.createSession();
        assertThat(dbSession.getInstanceId(), is("default"));
    }

    @Test
    public void testCreateWithInstancId() throws JasDBException, IOException {
        LocalDBSessionFactory localDBSessionFactory = new LocalDBSessionFactory();
        localDBSessionFactory.createSession().addInstance("testInstance");

        DBSession session = localDBSessionFactory.createSession("testInstance");
        assertThat(session.getInstanceId(), is("testInstance"));
    }

    @Test
    public void testCreateInstanceIdProperty() throws JasDBException, IOException {
        LocalDBSessionFactory localDBSessionFactory = new LocalDBSessionFactory();
        localDBSessionFactory.createSession().addInstance("testInstance");

        localDBSessionFactory.setInstance("testInstance");
        DBSession session = localDBSessionFactory.createSession();
        assertThat(session.getInstanceId(), is("testInstance"));
    }
}
