package nl.renarj.jasdb;

import nl.renarj.jasdb.api.DBSession;
import nl.renarj.jasdb.core.SimpleKernel;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.utils.HomeLocatorUtil;
import nl.renarj.storage.DBBaseTest;
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
        System.setProperty(HomeLocatorUtil.JASDB_HOME, DBBaseTest.tmpDir.toString());

        DBBaseTest.cleanData();
    }

    @After
    public void tearDown() throws Exception {
        SimpleKernel.shutdown();
        DBBaseTest.cleanData();
    }

    @Test
    public void testCreateDefaultSession() throws JasDBStorageException {
        LocalDBSessionFactory localDBSessionFactory = new LocalDBSessionFactory();
        DBSession dbSession = localDBSessionFactory.createSession();
        assertThat(dbSession.getInstanceId(), is("default"));
    }

    @Test
    public void testCreateWithInstancId() throws JasDBStorageException, IOException {
        LocalDBSessionFactory localDBSessionFactory = new LocalDBSessionFactory();
        localDBSessionFactory.createSession().addInstance("testInstance", temporaryFolder.newFolder().toString());

        DBSession session = localDBSessionFactory.createSession("testInstance");
        assertThat(session.getInstanceId(), is("testInstance"));
    }

    @Test
    public void testCreateInstanceIdProperty() throws JasDBStorageException, IOException {
        LocalDBSessionFactory localDBSessionFactory = new LocalDBSessionFactory();
        localDBSessionFactory.createSession().addInstance("testInstance", temporaryFolder.newFolder().toString());

        localDBSessionFactory.setInstance("testInstance");
        DBSession session = localDBSessionFactory.createSession();
        assertThat(session.getInstanceId(), is("testInstance"));
    }
}
