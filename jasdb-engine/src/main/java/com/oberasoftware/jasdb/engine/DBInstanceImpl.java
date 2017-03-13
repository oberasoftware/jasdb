/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package com.oberasoftware.jasdb.engine;

import com.oberasoftware.jasdb.api.session.DBInstance;
import com.oberasoftware.jasdb.api.model.Bag;
import com.oberasoftware.jasdb.api.model.Instance;
import com.oberasoftware.jasdb.api.engine.MetadataStore;
import com.oberasoftware.jasdb.api.exceptions.ConfigurationException;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;

import java.util.List;

public class DBInstanceImpl implements DBInstance {
    private final Instance instanceMeta;
    private final MetadataStore metadataStore;
    private final StorageServiceFactory storageServiceFactory;

	public DBInstanceImpl(StorageServiceFactory storageServiceFactory, MetadataStore metadataStore, Instance instanceMeta) {
	    this.storageServiceFactory = storageServiceFactory;
        this.instanceMeta = instanceMeta;
        this.metadataStore = metadataStore;
	}

	@Override
	public String getInstanceId() {
		return instanceMeta.getInstanceId();
	}
	
	@Override
	public List<Bag> getBags() throws JasDBStorageException {
        return metadataStore.getBags(instanceMeta.getInstanceId());
	}
	
	@Override
	public Bag getBag(String bagName) throws JasDBStorageException {
        return metadataStore.getBag(instanceMeta.getInstanceId(), bagName);
	}

    @Override
    public void removeBag(String bagName) throws JasDBStorageException {
        try {
            storageServiceFactory.removeStorageService(instanceMeta.getInstanceId(), bagName);
        } catch(ConfigurationException e) {
            throw new JasDBStorageException("Unable to remove bag", e);
        }
    }

    @Override
	public String getPath() {
		return instanceMeta.getPath();
	}
}
