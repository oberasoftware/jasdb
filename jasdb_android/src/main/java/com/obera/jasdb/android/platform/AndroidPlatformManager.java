package com.obera.jasdb.android.platform;

import android.content.Context;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.exceptions.RuntimeJasDBException;
import nl.renarj.jasdb.core.platform.PlatformManager;

/**
 * @author Renze de Vries
 */
public class AndroidPlatformManager implements PlatformManager {
    private static final String ANDROID_JVM_NAME = "dalvik";

    @Override
    public boolean platformMatch(String platformName) {
        return platformName.contains(ANDROID_JVM_NAME);
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
        return "" + android.os.Process.myPid();
    }

    @Override
    public void initializePlatform() throws JasDBStorageException {

    }

    @Override
    public void shutdownPlatform() throws JasDBStorageException {

    }
}
