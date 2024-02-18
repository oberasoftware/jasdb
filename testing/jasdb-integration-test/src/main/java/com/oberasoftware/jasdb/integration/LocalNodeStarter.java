package com.oberasoftware.jasdb.integration;

import com.oberasoftware.jasdb.api.exceptions.JasDBException;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.session.DBSession;
import com.oberasoftware.jasdb.api.session.EntityBag;
import com.oberasoftware.jasdb.core.SimpleEntity;
import com.oberasoftware.jasdb.service.JasDBMain;
import com.oberasoftware.jasdb.service.local.LocalDBSession;
import com.oberasoftware.jasdb.service.local.LocalDBSessionFactory;
import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author Renze de Vries
 */
public class LocalNodeStarter {
    private static final Logger LOG = getLogger(LocalNodeStarter.class);

    public static void main(String[] args) {
        try {
            LOG.info("Starting JasDB");
            JasDBMain.start();

            DBSession s = new LocalDBSessionFactory().createSession();
            s.getBags().forEach(b -> {
                try {
                    LOG.info("Bag: '{}'", b.getName());
                } catch (JasDBStorageException e) {
                    LOG.error("", e);
                }
            });
            LOG.info("JasDB was started, awaiting termination signal");

//            DBSession session = new LocalDBSession();
//            EntityBag bag = session.createOrGetBag("test");
//            bag.addEntity(new SimpleEntity().addProperty("testProperty", "someValue"));

            JasDBMain.waitForShutdown();
            LOG.info("JasDB was terminated");
        } catch (JasDBException e) {
            LOG.error("Could not start JasDB", e);
        }
    }
}
