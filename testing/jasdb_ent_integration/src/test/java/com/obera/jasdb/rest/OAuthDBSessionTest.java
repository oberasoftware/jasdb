package com.obera.jasdb.rest;

import com.obera.service.acl.BasicCredentials;
import com.oberasoftware.jasdb.core.acl.AccessMode;
import com.oberasoftware.jasdb.core.acl.SessionManager;
import com.oberasoftware.jasdb.core.acl.UserManager;
import com.oberasoftware.jasdb.core.acl.UserSession;
import com.oberasoftware.jasdb.engine.DBSessionTest;
import nl.renarj.jasdb.core.SimpleKernel;

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
