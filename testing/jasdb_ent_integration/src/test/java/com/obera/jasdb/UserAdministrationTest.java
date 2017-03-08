package com.obera.jasdb;

import com.obera.service.acl.BasicCredentials;
import com.oberasoftware.jasdb.engine.HomeLocatorUtil;
import nl.renarj.jasdb.SimpleBaseTest;
import nl.renarj.jasdb.api.DBSession;
import nl.renarj.jasdb.api.DBSessionFactory;
import nl.renarj.jasdb.api.UserAdministration;
import nl.renarj.jasdb.api.acl.AccessMode;
import nl.renarj.jasdb.api.acl.SessionManager;
import nl.renarj.jasdb.api.acl.UserManager;
import nl.renarj.jasdb.core.SimpleKernel;
import nl.renarj.jasdb.core.exceptions.JasDBException;
import nl.renarj.jasdb.core.exceptions.JasDBSecurityException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * @author Renze de Vries
 */
public abstract class UserAdministrationTest {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private DBSessionFactory sessionFactory;

    protected UserAdministrationTest(DBSessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;
    }

    @Before
    public void setUp() throws Exception {
        System.setProperty(HomeLocatorUtil.JASDB_HOME, SimpleBaseTest.tmpDir.toString());
        SimpleBaseTest.cleanData();
    }

    @After
    public void tearDown() throws Exception {
        SimpleKernel.shutdown();
        SimpleBaseTest.cleanData();
        System.setProperty(HomeLocatorUtil.JASDB_HOME, "");
    }

    @Test
    public void testAddUser() throws JasDBException {
        DBSession session = sessionFactory.createSession();
        UserAdministration userAdministration = session.getUserAdministration();

        userAdministration.addUser("test", "*", "1234");
        List<String> users = userAdministration.getUsers();
        assertThat(users, hasItems("admin", "test"));
    }

    @Test
    public void testDeleteUser() throws JasDBException {
        DBSession session = sessionFactory.createSession();
        UserAdministration userAdministration = session.getUserAdministration();

        userAdministration.addUser("test", "*", "1234");
        assertThat(userAdministration.getUsers(), hasItems("admin", "test"));
        userAdministration.deleteUser("test");
        assertThat(userAdministration.getUsers(), hasItems("admin"));
    }

    @Test
    public void testGrantUser() throws JasDBException {
        DBSession session = sessionFactory.createSession();
        UserAdministration userAdministration = session.getUserAdministration();

        userAdministration.addUser("test", "*", "1234");
        userAdministration.grant("test", "/", AccessMode.CONNECT);
        userAdministration.grant("test", "/Instances", AccessMode.WRITE);

        UserManager userManager = SimpleKernel.getKernelModule(UserManager.class);
        SessionManager sessionManager = SimpleKernel.getKernelModule(SessionManager.class);
        userManager.authorize(sessionManager.startSession(new BasicCredentials("test", "localhost", "1234")), "/Instances", AccessMode.READ);
    }

    @Test
    public void testRevokeGrant() throws JasDBException {
        expectedException.expect(JasDBSecurityException.class);
        expectedException.expectMessage("User: test has insufficient privileges on object: /");

        DBSession session = sessionFactory.createSession();
        UserAdministration userAdministration = session.getUserAdministration();

        userAdministration.addUser("test", "*", "1234");
        userAdministration.grant("test", "/", AccessMode.ADMIN);

        UserManager userManager = SimpleKernel.getKernelModule(UserManager.class);
        SessionManager sessionManager = SimpleKernel.getKernelModule(SessionManager.class);

        userManager.authorize(sessionManager.startSession(new BasicCredentials("test", "localhost", "1234")), "/Instances", AccessMode.READ);

        userAdministration.revoke("test", "/");

        userManager.authorize(sessionManager.startSession(new BasicCredentials("test", "localhost", "1234")), "/Instances", AccessMode.READ);

        fail("Should no longer be authorized");
    }
}
