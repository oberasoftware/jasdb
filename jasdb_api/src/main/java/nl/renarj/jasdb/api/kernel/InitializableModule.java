package nl.renarj.jasdb.api.kernel;

import nl.renarj.jasdb.core.exceptions.JasDBStorageException;

/**
 * @author Renze de Vries
 */
public interface InitializableModule {
    void initialize(KernelContext context) throws JasDBStorageException;
}
