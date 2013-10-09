package com.obera.service.acl;

import com.google.inject.Injector;
import nl.renarj.jasdb.SimpleBaseTest;
import nl.renarj.jasdb.api.acl.AccessMode;
import nl.renarj.jasdb.api.acl.SessionManager;
import nl.renarj.jasdb.api.acl.UserManager;
import nl.renarj.jasdb.api.acl.UserSession;
import nl.renarj.jasdb.api.context.Credentials;
import nl.renarj.jasdb.api.context.RequestContext;
import nl.renarj.jasdb.api.kernel.KernelContext;
import nl.renarj.jasdb.api.metadata.MetadataStore;
import nl.renarj.jasdb.core.SimpleKernel;
import nl.renarj.jasdb.core.exceptions.JasDBException;
import nl.renarj.jasdb.core.exceptions.JasDBSecurityException;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.utils.HomeLocatorUtil;
import nl.renarj.jasdb.service.StorageService;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Renze de Vries
 */
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

    private AuthorizationServiceWrapper authService;

    private StorageService wrappedService;

    private SessionManager sessionManager;
    private UserManager userManager;

    private UserSession adminSession;

    private AccessMode grantedMode;
    private AccessMode notGrantedMode;

    protected AbstractAuthorizationTest(AccessMode notGrantedMode, AccessMode grantedMode) {
        this.grantedMode = grantedMode;
        this.notGrantedMode = notGrantedMode;
    }

    protected abstract AuthorizationOperation getOperation();

    @Before
    public void before() throws JasDBStorageException {
        System.setProperty(HomeLocatorUtil.JASDB_HOME, SimpleBaseTest.tmpDir.toString());
        SimpleBaseTest.cleanData();

        SimpleKernel.initializeKernel();

        MetadataStore metadataStore = SimpleKernel.getMetadataStore();

        LocalCredentialsProvider localCredentialsProvider = new LocalCredentialsProvider();
        userManager = new UserManagerImpl(localCredentialsProvider);
        sessionManager = new SessionManagerImpl(userManager);
        KernelContext kernelContext = mock(KernelContext.class);
        Injector injector = mock(Injector.class);
        wrappedService = mock(StorageService.class);
        when(wrappedService.getInstanceId()).thenReturn(TEST_INSTANCE);
        when(wrappedService.getBagName()).thenReturn(TEST_BAG);

        when(kernelContext.getInjector()).thenReturn(injector);
        when(injector.getInstance(UserManager.class)).thenReturn(userManager);
        when(kernelContext.getMetadataStore()).thenReturn(metadataStore);

        localCredentialsProvider.initialize(kernelContext);
        authService = new AuthorizationServiceWrapper();
        authService.wrap(kernelContext, wrappedService);

        adminSession = sessionManager.startSession(new BasicCredentials(ADMIN_USER, LOCALHOST, DEFAULT_ADMIN_PASS));
    }

    @After
    public void after() throws JasDBException {
        SimpleKernel.shutdown();
        SimpleBaseTest.cleanData();
    }

    @Test
    public void testOperationAdminGranted() throws JasDBStorageException {
        getOperation().doOperation(authService, wrappedService, "admin", "");
    }

    @Test(expected = JasDBSecurityException.class)
    public void testOperationNotGranted() throws JasDBStorageException {
        createUser("notGranted", "1234");

        getOperation().doOperation(authService, wrappedService, TEST_USER, "1234");
    }

    @Test
    public void testOperationInsufficientPermission() throws JasDBStorageException {
        expectedException.expect(JasDBSecurityException.class);
        expectedException.expectMessage("insufficient privileges");

        createUser(TEST_USER, "1234");
        createGrant("/", TEST_USER, AccessMode.READ); //grant at least connect permissions
        createGrant("/" + TEST_INSTANCE + "/bags/" + TEST_BAG, TEST_USER, notGrantedMode);

        getOperation().doOperation(authService, wrappedService, TEST_USER, "1234");
    }

    @Test
    public void testOperationSufficientPermission() throws JasDBStorageException {
        createUser(TEST_USER, "1234");
        createGrant("/", TEST_USER, AccessMode.READ); //grant at least connect permissions
        createGrant("/" + TEST_INSTANCE + "/bags/" + TEST_BAG, TEST_USER, grantedMode);

        getOperation().doOperation(authService, wrappedService, TEST_USER, "1234");
    }

    @Test
    public void testOperationParentPermission() throws JasDBStorageException {
        createUser(TEST_USER, "1234");
        createGrant("/", TEST_USER, AccessMode.READ); //grant at least connect permissions
        createGrant("/" + TEST_INSTANCE, TEST_USER, grantedMode);

        getOperation().doOperation(authService, wrappedService, TEST_USER, "1234");
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
