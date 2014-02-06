package com.obera.jasdb.android;

import nl.renarj.jasdb.api.model.IndexManager;

/**
 * @author renarj
 */
public interface GuiceIndexManagerFactory {
    IndexManager getIndexManager(String instance);
}
