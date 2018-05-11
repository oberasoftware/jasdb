package com.oberasoftware.jasdb.service;

import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author Renze de Vries
 */
public class KernelShutdown implements Runnable, KernelShutdownMBean {
    private static final Logger LOG = getLogger(KernelShutdown.class);

    @Override
    public void doKernelShutdown() {
        run();
    }

    @Override
    public void run() {
        LOG.info("Running kernel shutdown");
        JasDBMain.shutdown();
    }
}
