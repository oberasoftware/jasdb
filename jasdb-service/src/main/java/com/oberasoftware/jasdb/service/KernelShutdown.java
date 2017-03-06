package com.oberasoftware.jasdb.service;

import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author renarj
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
    }
}
