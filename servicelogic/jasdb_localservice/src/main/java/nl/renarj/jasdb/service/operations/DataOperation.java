package nl.renarj.jasdb.service.operations;

import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;

public interface DataOperation {
	public void doDataOperation(String instanceId, String bag, SimpleEntity entity) throws JasDBStorageException;
}
