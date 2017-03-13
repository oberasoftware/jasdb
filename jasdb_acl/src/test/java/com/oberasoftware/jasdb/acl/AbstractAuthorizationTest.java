package com.oberasoftware.jasdb.acl;

import com.oberasoftware.jasdb.engine.HomeLocatorUtil;
import com.oberasoftware.jasdb.engine.StorageService;
import com.oberasoftware.jasdb.engine.StorageServiceFactory;
import com.oberasoftware.jasdb.api.security.AccessMode;
import com.oberasoftware.jasdb.api.security.SessionManager;
import com.oberasoftware.jasdb.api.security.UserManager;
import com.oberasoftware.jasdb.api.security.UserSession;
import com.oberasoftware.jasdb.api.security.Credentials;
import com.oberasoftware.jasdb.core.context.RequestContext;
import com.oberasoftware.jasdb.api.exceptions.JasDBException;
import com.oberasoftware.jasdb.api.exceptions.JasDBSecurityException;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.IOException;

import static org.mockito.Mockito.when;

/**
 * @author Renze de Vries
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"/jasdb-security-test.xml"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public abstract class AbstractAuthorizationTest {
    private static final String DEFAULT_ADMIN_PASS = "";
    private static final String ADMIN_USER = "admin";
    private static final String TEST_INSTANCE = "INSTANCE1";
    private static final String TEST_BAG = "BAG1";
    private static final String LOCALHOST = "localhost";
    private static final String TEST_USER = "testuser";
    private static final String TEST_PASSWORD = "1234";

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @ClassRule
    public static TemporaryFolder temporaryFolder = new TemporaryFolder();

    private StorageService wrappedService;

    @Autowired
    private SessionManager sessionManager;

    @Autowired
    private StorageServiceFactory storageServiceFactory;

    @Autowired
    private UserManager userManager;

    private UserSession adminSession;

    private AccessMode grantedMode;
    private AccessMode notGrantedMode;

    AbstractAuthorizationTest(AccessMode notGrantedMode, AccessMode grantedMode) {
        this.grantedMode = grantedMode;
        this.notGrantedMode = notGrantedMode;
    }

    protected abstract AuthorizationOperation getOperation();

    @BeforeClass
    public static void beforeClass() throws IOException {
        System.setProperty(HomeLocatorUtil.JASDB_HOME, temporaryFolder.newFolder().toString());
    }

    @Before
    public void before() throws JasDBStorageException, IOException {
        wrappedService = storageServiceFactory.getStorageService(null, null);
        when(wrappedService.getInstanceId()).thenReturn(TEST_INSTANCE);
        when(wrappedService.getBagName()).thenReturn(TEST_BAG);

        adminSession = sessionManager.startSession(new BasicCredentials(ADMIN_USER, LOCALHOST, DEFAULT_ADMIN_PASS));
    }

    @After
    public void after() throws JasDBException {
        temporaryFolder.delete();
    }

    @Test
    public void testOperationAdminGranted() throws Exception {
        getOperation().doOperation(wrappedService, "admin", "");
    }

    @Test
    public void testOperationNotGranted() throws Exception {
        expectedException.expect(JasDBSecurityException.class);
        expectedException.expectMessage("notGranted has insufficient privileges");

        createUser("notGranted");

        getOperation().doOperation(wrappedService, "notGranted", TEST_PASSWORD);
    }

    @Test
    public void testOperationInsufficientPermission() throws Exception {
        expectedException.expect(JasDBSecurityException.class);
        expectedException.expectMessage("insufficient privileges");

        createUser(TEST_USER);
        createGrant("/", TEST_USER, AccessMode.READ); //grant at least connect permissions
        createGrant("/" + TEST_INSTANCE + "/bags/" + TEST_BAG, TEST_USER, notGrantedMode);

        getOperation().doOperation(wrappedService, TEST_USER, TEST_PASSWORD);
    }

    @Test
    public void testOperationSufficientPermission() throws Exception {
        createUser(TEST_USER);
        createGrant("/", TEST_USER, AccessMode.READ); //grant at least connect permissions
        createGrant("/" + TEST_INSTANCE + "/bags/" + TEST_BAG, TEST_USER, grantedMode);

        getOperation().doOperation(wrappedService, TEST_USER, TEST_PASSWORD);
    }

    @Test
    public void testOperationParentPermission() throws Exception {
        createUser(TEST_USER);
        createGrant("/", TEST_USER, AccessMode.READ); //grant at least connect permissions
        createGrant("/" + TEST_INSTANCE, TEST_USER, grantedMode);

        getOperation().doOperation(wrappedService, TEST_USER, TEST_PASSWORD);
    }

    RequestContext createContext(String userName, String password) throws JasDBStorageException {
        RequestContext context = new RequestContext(true, true);
        Credentials credentials = new BasicCredentials(userName, "localhost", password);
        UserSession session = sessionManager.startSession(credentials);
        context.setUserSession(session);

        return context;
    }

    private void createGrant(String object, String user, AccessMode mode) throws JasDBStorageException {
        userManager.grantUser(adminSession, object, user, mode);
    }

    private void createUser(String userName) throws JasDBStorageException {
        userManager.addUser(adminSession, userName, "localhost", TEST_PASSWORD);
    }

}
