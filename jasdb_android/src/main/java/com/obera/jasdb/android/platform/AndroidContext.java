package com.obera.jasdb.android.platform;

import android.content.Context;
import nl.renarj.jasdb.core.platform.PlatformManagerFactory;

/**
 * @author Renze de Vries
 */
public class AndroidContext {
    private static AndroidContext INSTANCE;

    private Context context;
    static {
        PlatformManagerFactory.setPlatformManager(new AndroidPlatformManager());
    }

    public AndroidContext(Context context) {
        this.context = context;
    }

    public static void setContext(Context context) {
        if(INSTANCE == null) {
            initializeContext(context);
        }
    }

    public static Context getContext() {
        if(INSTANCE != null) {
            return INSTANCE.context;
        } else {
            return null;
        }
    }

    private static synchronized void initializeContext(Context context) {
        if(INSTANCE == null) {
            INSTANCE = new AndroidContext(context);
        }
    }
}
