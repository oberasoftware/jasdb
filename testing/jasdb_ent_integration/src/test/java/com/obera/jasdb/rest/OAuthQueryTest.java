package com.obera.jasdb.rest;

import com.obera.service.acl.BasicCredentials;
import com.oberasoftware.jasdb.engine.EntityQueryTest;
import com.oberasoftware.jasdb.engine.HomeLocatorUtil;
import nl.renarj.jasdb.SimpleBaseTest;
import com.oberasoftware.jasdb.core.acl.AccessMode;
import com.oberasoftware.jasdb.core.acl.SessionManager;
import com.oberasoftware.jasdb.core.acl.UserManager;
import com.oberasoftware.jasdb.core.acl.UserSession;
import nl.renarj.jasdb.core.SimpleKernel;

/**
 * @author Renze de Vries
 */
public class OAuthQueryTest extends EntityQueryTest {
    public OAuthQueryTest() {
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
        System.setProperty(HomeLocatorUtil.JASDB_HOME, SimpleBaseTest.tmpDir.toString());

        SimpleKernel.initializeKernel();

        SessionManager sessionManager = SimpleKernel.getKernelModule(SessionManager.class);
        UserManager userManager = SimpleKernel.getKernelModule(UserManager.class);
        UserSession session = sessionManager.startSession(new BasicCredentials("admin", "localhost", ""));
        userManager.addUser(session, "remote", "*", "1234");
        userManager.grantUser(session, "/", "remote", AccessMode.ADMIN);

        super.setUp();
    }
}
