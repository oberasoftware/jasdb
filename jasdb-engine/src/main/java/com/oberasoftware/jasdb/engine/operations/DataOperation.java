package com.oberasoftware.jasdb.engine.operations;

import com.oberasoftware.jasdb.api.session.Entity;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;

public interface DataOperation {
	void doDataOperation(String instanceId, String bag, Entity entity) throws JasDBStorageException;
}
