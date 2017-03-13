package com.oberasoftware.jasdb.integration;

import com.oberasoftware.jasdb.service.JasDBMain;
import com.oberasoftware.jasdb.api.exceptions.JasDBException;
import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author renarj
 */
public class LocalNodeStarter {
    private static final Logger LOG = getLogger(LocalNodeStarter.class);

    public static void main(String[] args) {
        try {
            LOG.info("Starting JasDB");
            JasDBMain.start();

            LOG.info("JasDB was started, awaiting termination signal");

            JasDBMain.waitForShutdown();
            LOG.info("JasDB was terminated");
        } catch (JasDBException e) {
            LOG.error("Could not start JasDB", e);
        }
    }
}
