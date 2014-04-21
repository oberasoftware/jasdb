package com.obera.jasdb.android.platform;

import android.content.Context;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.obera.jasdb.android.AndroidKernelBinding;
import nl.renarj.jasdb.api.metadata.MetadataStore;
import nl.renarj.jasdb.api.model.IndexManagerFactory;
import nl.renarj.jasdb.core.exceptions.ConfigurationException;
import nl.renarj.jasdb.core.exceptions.JasDBException;
import nl.renarj.jasdb.core.exceptions.NoComponentFoundException;
import nl.renarj.jasdb.core.exceptions.RuntimeJasDBException;
import nl.renarj.jasdb.core.platform.PlatformManager;
import nl.renarj.jasdb.service.StorageServiceFactory;
import nl.renarj.jasdb.storage.RecordWriterFactoryLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author Renze de Vries
 */
public class AndroidPlatformManager implements PlatformManager {
    private static final Logger LOG = LoggerFactory.getLogger(AndroidPlatformManager.class);

    private static final String ANDROID_JVM_NAME = "dalvik";
    private static final String JASDB_ANDROID = "JasDB For Android";

    private Injector injector;

    @Override
    public boolean platformMatch() {
        return System.getProperty("java.vm.name").contains(ANDROID_JVM_NAME);
    }

    @Override
    public String getDefaultStorageLocation() {
        Context context = AndroidContext.getContext();
        if(context != null) {
            return context.getFilesDir().toString();
        } else {
            throw new RuntimeJasDBException("No Android application context available, please use AndroidDBSession for DB session initialization");
        }
    }

    @Override
    public String getProcessId() {
        return "" + System.currentTimeMillis();
    }

    @Override
    public void initializePlatform() throws ConfigurationException {
        LOG.info("Initializing platform: {}", this.hashCode());
        this.injector = Guice.createInjector(new AndroidKernelBinding());
    }

    @Override
    public void shutdownPlatform() throws JasDBException {
        LOG.info("shutting down platform: {}", this.hashCode());
        getComponent(MetadataStore.class).closeStore();
        getComponent(StorageServiceFactory.class).shutdownServiceFactory();
        getComponent(RecordWriterFactoryLoader.class).closeRecordWriters();
        getComponent(IndexManagerFactory.class).shutdownIndexes();

        this.injector = null;
    }

    @Override
    public <T> T getComponent(Class<T> type) {
        return injector.getInstance(type);
    }

    @Override
    public <T> List<T> getComponents(Class<T> type) throws NoComponentFoundException {
        throw new NoComponentFoundException("No support for multi-component architecture");
    }

    @Override
    public String getVersionData() {
        return JASDB_ANDROID;
    }
}
