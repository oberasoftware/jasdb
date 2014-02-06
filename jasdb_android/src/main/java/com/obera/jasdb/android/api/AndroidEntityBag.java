package com.obera.jasdb.android.api;

import nl.renarj.jasdb.core.SimpleKernel;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.storage.entities.EntityBagImpl;

/**
 * @author renarj
 */
public class AndroidEntityBag extends EntityBagImpl {
    public AndroidEntityBag(String dbInstance, String bag) throws JasDBStorageException {
        super(dbInstance, bag,
                SimpleKernel.getStorageServiceFactory().getOrCreateStorageService(dbInstance, bag));

    }


}
