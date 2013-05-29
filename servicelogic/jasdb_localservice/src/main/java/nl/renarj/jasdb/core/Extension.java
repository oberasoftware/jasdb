package nl.renarj.jasdb.core;

import nl.renarj.jasdb.api.kernel.KernelContext;
import nl.renarj.jasdb.core.exceptions.ConfigurationException;

/**
 * @author Renze de Vries
 */
public interface Extension {
    void load(KernelContext kernelContext) throws ConfigurationException;

    void shutdown(KernelContext kernelContext) throws ConfigurationException;
}
