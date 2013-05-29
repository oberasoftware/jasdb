package nl.renarj.jasdb.service.wrappers;

import nl.renarj.jasdb.api.kernel.KernelContext;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.service.StorageService;

/**
 * @author Renze de Vries
 */
public interface ServiceWrapper extends StorageService {
    /**
     * Wraps the embedded storage service by the functionality of the wrapper, this
     * means all calls will first go through the wrapper before entering the wrapped
     * service.
     *
     * @param storageService The storage service to wrap
     * @throws JasDBStorageException If unable to wrap the provided storage service
     */
    void wrap(KernelContext kernelContext, StorageService storageService) throws JasDBStorageException;

    /**
     * Unwraps the embedded storage service
     * @return The embedded wrapped storage service
     * @throws JasDBStorageException If unable to unwrap the storage service
     */
    StorageService unwrap() throws JasDBStorageException;
}
