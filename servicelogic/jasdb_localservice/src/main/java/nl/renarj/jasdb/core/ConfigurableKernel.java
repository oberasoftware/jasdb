package nl.renarj.jasdb.core;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;
import com.google.inject.matcher.Matcher;
import com.google.inject.spi.TypeListener;
import nl.renarj.core.utilities.StringUtils;
import nl.renarj.core.utilities.configuration.Configuration;
import nl.renarj.jasdb.api.acl.CredentialsProvider;
import nl.renarj.jasdb.api.acl.SessionManager;
import nl.renarj.jasdb.api.acl.UserManager;
import nl.renarj.jasdb.api.metadata.MetadataStore;
import nl.renarj.jasdb.api.model.DBInstanceFactory;
import nl.renarj.jasdb.core.dummy.DummyCredentialsProvider;
import nl.renarj.jasdb.core.dummy.DummyRemoteService;
import nl.renarj.jasdb.core.dummy.DummySessionManager;
import nl.renarj.jasdb.core.dummy.DummyUserManager;
import nl.renarj.jasdb.core.exceptions.RuntimeJasDBException;
import nl.renarj.jasdb.core.storage.RecordWriterFactory;
import nl.renarj.jasdb.service.LocalStorageServiceFactoryImpl;
import nl.renarj.jasdb.service.StorageServiceFactory;
import nl.renarj.jasdb.service.metadata.JasDBMetadataStore;
import nl.renarj.jasdb.storage.transactional.TransactionalRecordWriterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This is the configurable kernel module which reads the bindings from the configuration file instead of
 * compile time determined bindings.
 *
 * @author Renze de Vries
 */
public class ConfigurableKernel extends AbstractModule {
    private static final Logger LOG = LoggerFactory.getLogger(ConfigurableKernel.class);

    private static final String CREDENTIALS_PROVIDER = "credentialsprovider";
    private static final String REMOTE_MODULE = "remote";
    private static final String RECORD_MODULE = "record";
    private static final String STORAGESERVICE_MODULE = "storageservice";
    private static final String USER_MANAGER_MODULE = "usermanager";
    private static final String SESSION_MANAGER_MODULE = "sessionmanager";

    private Configuration configuration;

    public ConfigurableKernel(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    protected void bindListener(Matcher<? super TypeLiteral<?>> typeMatcher, TypeListener listener) {
        super.bindListener(typeMatcher, listener);
    }

    @Override
    protected void configure() {
        List<Configuration> moduleConfigurations = configuration.getChildConfigurations("/jasdb/modules/module");
        Map<String, String> modules = new HashMap<String, String>();
        for(Configuration moduleConfiguration : moduleConfigurations) {
            String moduleType = moduleConfiguration.getAttribute("type");
            String moduleImplementation = moduleConfiguration.getAttribute("class");

            if(StringUtils.stringNotEmpty(moduleType) && StringUtils.stringNotEmpty(moduleImplementation)) {
                modules.put(moduleType, moduleImplementation);
            }
        }

        bind(DBInstanceFactory.class).to(DBInstanceFactoryImpl.class);
        bind(MetadataStore.class).to(JasDBMetadataStore.class);
        loadModule(StorageServiceFactory.class, STORAGESERVICE_MODULE, modules, LocalStorageServiceFactoryImpl.class);
        loadModule(RemoteService.class, REMOTE_MODULE, modules, DummyRemoteService.class);
        loadModule(RecordWriterFactory.class, RECORD_MODULE, modules, TransactionalRecordWriterFactory.class);

        loadModule(UserManager.class, USER_MANAGER_MODULE, modules, DummyUserManager.class);
        loadModule(SessionManager.class, SESSION_MANAGER_MODULE, modules, DummySessionManager.class);
        loadModule(CredentialsProvider.class, CREDENTIALS_PROVIDER, modules, DummyCredentialsProvider.class);
    }

    private <T> void loadModule(Class<T> moduleBindType, String moduleName, Map<String, String> modules, Class<? extends T> defaultBinding) {
        if(modules.containsKey(moduleName)) {
            String implementation = modules.get(moduleName);
            doGenericBind(implementation, moduleBindType);
        } else {
            bind(moduleBindType).to(defaultBinding);
        }
    }

    private <T> void doGenericBind(String implementation, Class<T> bindType) {
        try {
            Class<? extends T> moduleClass = Class.forName(implementation).asSubclass(bindType);
            bind(bindType).to(moduleClass);
            LOG.debug("Using configured module: {}", implementation);
        } catch(ClassNotFoundException e) {
            throw new RuntimeJasDBException("Unable to load custom module: " + implementation, e);
        }
    }

    @Provides
    Configuration provideConfiguration() {
        return this.configuration;
    }
}
