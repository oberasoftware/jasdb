package com.obera.jasdb.rest;

import com.obera.service.acl.BasicCredentials;
import nl.renarj.jasdb.api.acl.AccessMode;
import nl.renarj.jasdb.api.acl.SessionManager;
import nl.renarj.jasdb.api.acl.UserManager;
import nl.renarj.jasdb.api.acl.UserSession;
import nl.renarj.jasdb.core.SimpleKernel;
import nl.renarj.jasdb.service.DBSessionTest;

/**
 * @author Renze de Vries
 */
public class OAuthDBSessionTest extends DBSessionTest {
    public OAuthDBSessionTest() {
        super(new TestRestDBSessionFactory(new BasicCredentials("remote", "1234")));
    }

    @Override
    public void tearDown() throws Exception {
        System.setProperty("jasdb-config", "");
        super.tearDown();
    }

    @Override
    public void setUp() throws Exception {
        System.setProperty("jasdb-config", "jasdb-rest-withsecurity.xml");

        super.setUp();

        SimpleKernel.initializeKernel();

        SessionManager sessionManager = SimpleKernel.getKernelModule(SessionManager.class);
        UserManager userManager = SimpleKernel.getKernelModule(UserManager.class);
        UserSession session = sessionManager.startSession(new BasicCredentials("admin", "localhost", ""));
        userManager.addUser(session, "remote", "*", "1234");
        userManager.grantUser(session, "/", "remote", AccessMode.ADMIN);
    }
}
