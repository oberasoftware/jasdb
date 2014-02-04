package com.obera.service.acl;

import nl.renarj.jasdb.SimpleBaseTest;
import nl.renarj.jasdb.api.acl.AccessMode;
import nl.renarj.jasdb.api.acl.SessionManager;
import nl.renarj.jasdb.api.acl.UserManager;
import nl.renarj.jasdb.api.acl.UserSession;
import nl.renarj.jasdb.api.context.Credentials;
import nl.renarj.jasdb.api.context.RequestContext;
import nl.renarj.jasdb.core.SimpleKernel;
import nl.renarj.jasdb.core.exceptions.JasDBException;
import nl.renarj.jasdb.core.exceptions.JasDBSecurityException;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.platform.HomeLocatorUtil;
import nl.renarj.jasdb.service.StorageService;
import nl.renarj.jasdb.service.StorageServiceFactory;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.mockito.Mockito.when;

/**
 * @author Renze de Vries
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"/jasdb-security-test.xml"})
public abstract class AbstractAuthorizationTest {
    private static final String DEFAULT_ADMIN_PASS = "";
    private static final String ADMIN_USER = "admin";
    private static final String TEST_INSTANCE = "INSTANCE1";
    private static final String TEST_BAG = "BAG1";
    private static final String LOCALHOST = "localhost";
    public static final String TEST_USER = "testuser";

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

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

    protected AbstractAuthorizationTest(AccessMode notGrantedMode, AccessMode grantedMode) {
        this.grantedMode = grantedMode;
        this.notGrantedMode = notGrantedMode;
    }

    protected abstract AuthorizationOperation getOperation();

    @BeforeClass
    public static void beforeSetup() {
        System.setProperty(HomeLocatorUtil.JASDB_HOME, SimpleBaseTest.tmpDir.toString());
    }

    @Before
    public void before() throws JasDBStorageException {
        SimpleBaseTest.cleanData();
//        SimpleKernel.initializeKernel();

//        MetadataStore metadataStore = SimpleKernel.getKernelModule(MetadataStore.class);

//        LocalCredentialsProvider localCredentialsProvider = new LocalCredentialsProvider();
//        userManager = SimpleKernel.getKernelModule(UserManager.class);
//        sessionManager = SimpleKernel.getKernelModule(SessionManager.class);

//        StorageServiceFactory storageServiceFactory = SimpleKernel.getKernelModule(StorageServiceFactory.class);
        wrappedService = storageServiceFactory.getStorageService(null, null);
        when(wrappedService.getInstanceId()).thenReturn(TEST_INSTANCE);
        when(wrappedService.getBagName()).thenReturn(TEST_BAG);

//        localCredentialsProvider.initialize(kernelContext);
//        authService = new AuthorizationServiceWrapper();
//        authService.wrap(kernelContext, wrappedService);

        adminSession = sessionManager.startSession(new BasicCredentials(ADMIN_USER, LOCALHOST, DEFAULT_ADMIN_PASS));
    }

    @After
    public void after() throws JasDBException {
        SimpleKernel.shutdown();
        SimpleBaseTest.cleanData();
    }

    @Test
    public void testOperationAdminGranted() throws Exception {
        getOperation().doOperation(wrappedService, "admin", "");
    }

    @Test
    public void testOperationNotGranted() throws Exception {
        expectedException.expect(JasDBSecurityException.class);
        expectedException.expectMessage("notGranted has insufficient privileges");

        createUser("notGranted", "1234");

        getOperation().doOperation(wrappedService, "notGranted", "1234");
    }

    @Test
    public void testOperationInsufficientPermission() throws Exception {
        expectedException.expect(JasDBSecurityException.class);
        expectedException.expectMessage("insufficient privileges");

        createUser(TEST_USER, "1234");
        createGrant("/", TEST_USER, AccessMode.READ); //grant at least connect permissions
        createGrant("/" + TEST_INSTANCE + "/bags/" + TEST_BAG, TEST_USER, notGrantedMode);

        getOperation().doOperation(wrappedService, TEST_USER, "1234");
    }

    @Test
    public void testOperationSufficientPermission() throws Exception {
        createUser(TEST_USER, "1234");
        createGrant("/", TEST_USER, AccessMode.READ); //grant at least connect permissions
        createGrant("/" + TEST_INSTANCE + "/bags/" + TEST_BAG, TEST_USER, grantedMode);

        getOperation().doOperation(wrappedService, TEST_USER, "1234");
    }

    @Test
    public void testOperationParentPermission() throws Exception {
        createUser(TEST_USER, "1234");
        createGrant("/", TEST_USER, AccessMode.READ); //grant at least connect permissions
        createGrant("/" + TEST_INSTANCE, TEST_USER, grantedMode);

        getOperation().doOperation(wrappedService, TEST_USER, "1234");
    }

    protected RequestContext createContext(String userName, String password, String host) throws JasDBStorageException {
        RequestContext context = new RequestContext(true, true);
        Credentials credentials = new BasicCredentials(userName, host, password);
        UserSession session = sessionManager.startSession(credentials);
        context.setUserSession(session);

        return context;
    }

    private void createGrant(String object, String user, AccessMode mode) throws JasDBStorageException {
        userManager.grantUser(adminSession, object, user, mode);
    }

    private void createUser(String userName, String password) throws JasDBStorageException {
        userManager.addUser(adminSession, userName, "localhost", password);
    }

}
