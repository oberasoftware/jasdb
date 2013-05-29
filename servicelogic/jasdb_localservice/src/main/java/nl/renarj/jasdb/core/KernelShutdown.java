package nl.renarj.jasdb.core;

import nl.renarj.jasdb.core.exceptions.JasDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author: renarj
 * Date: 2-5-12
 * Time: 19:44
 */
public class KernelShutdown implements KernelShutdownMBean, Runnable {
    private static final Logger log = LoggerFactory.getLogger(KernelShutdown.class);

    @Override
    public void doKernelShutdown() {
        run();
    }

    @Override
    public void run() {
        try {
            SimpleKernel.shutdown();
        } catch(JasDBException e) {
            log.error("Unable to do JMX kernel shutdown", e);
        }
    }
}
