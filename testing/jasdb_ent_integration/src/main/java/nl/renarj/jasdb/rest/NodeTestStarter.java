package nl.renarj.jasdb.rest;

import nl.renarj.jasdb.LocalDBSession;
import nl.renarj.jasdb.api.DBSession;
import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.api.model.EntityBag;
import nl.renarj.jasdb.core.SimpleKernel;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.rest.client.ResourceNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NodeTestStarter {
	private static final Logger LOG = LoggerFactory.getLogger(NodeTestStarter.class);
	
	public static void main(String[] args) {
		LOG.info("Starting data preperation for test service");
        //System.setProperty("test.grid.enabled", "true");
//		DataPrepare.doDataPrepare();
        //SimpleBaseTest.cleanData();
		
		try {
			LOG.info("Starting kernel");
            //System.setProperty("test.discovery.enabled", "true");
            //System.setProperty("test.grid.enabled", "true");
			SimpleKernel.initializeKernel();

//            EntityBag bag = new LocalDBSession("test").createOrGetBag("blabla");
//            bag.addEntity(new SimpleEntity());
//            bag.addEntity(new SimpleEntity());
//            bag.addEntity(new SimpleEntity());


//            SessionManager sessionManager = SimpleKernel.getKernelModule(SessionManager.class);
//            UserSession userSession = sessionManager.startSession(new BasicCredentials("admin", "localhost", ""));
//            UserManager userManager = SimpleKernel.getKernelModule(UserManager.class);
//            userManager.addUser(userSession, "test", "*", "1234");
//            userManager.addUser(userSession, "test2", "*", "1234");
//
//            userManager.grantUser(userSession, "/", "test", AccessMode.READ);
//            userManager.grantUser(userSession, "/", "test2", AccessMode.READ);
//            userManager.grantUser(userSession, "/default/bags/test", "test", AccessMode.NONE);

//            DBSession session = new LocalDBSession(new BasicCredentials("test", "localhost", "1234"));
//            EntityBag bag = session.createOrGetBag("test");
//            bag.addEntity(new SimpleEntity());

//            DBSession session = new RestDBSession("default", "localhost", new BasicCredentials("test", "1234"), 7051, false);
//            EntityBag bag = session.createOrGetBag("test");
//            bag.addEntity(new SimpleEntity());

//            DBInstanceFactory instanceFactory = SimpleKernel.getInstanceFactory();
//            instanceFactory.addInstance("myInstanceId", "/tmp/location");
//            DBSession session = new LocalDBSession("myInstanceId");

//            DBSession session = new LocalDBSession();
//            EntityBag bag = session.getBag("wikidata");

//            QueryResult result = bag.getEntities();
//            for(SimpleEntity entity : result) {
//                Property property = entity.getProperty("languages");
//                for(Object v : property.getValueObjects()) {
//                    LOG.info("Language: {}", v);
//                }
//            }

//			DBSession session = new LocalDBSession();
//			EntityBag bag = session.createOrGetBag("items");
//					bag.ensureIndex(
//							new CompositeIndexField(
//									new IndexField("controllerId", new StringKeyType()),
//									new IndexField("pluginId", new StringKeyType()),
//									new IndexField("deviceId", new StringKeyType()),
//									new IndexField("type", new StringKeyType())
//							), false);
//
//			SimpleEntity entity = new SimpleEntity(UUID.randomUUID().toString())
//					.addProperty("controllerId", "Renzes-MacBook-Pro-2.local").addProperty("pluginId", "zwave")
//					.addProperty("name", "ZWave provider").addProperty("deviceId", "13").addProperty("type", "device");
//
//			createOrUpdate(entity, "items");
//
//			LOG.info("Items in bag: {}", bag.getSize());
//
//			createOrUpdate(entity, "items");
//
//			LOG.info("Items in bag after update: {}", bag.getSize());

//            SimpleKernel.shutdown();
            SimpleKernel.waitForShutdown();
		} catch (JasDBStorageException e) {
            LOG.error("Unable to start kernel", e);
        }
//        catch(JasDBStorageException e) {
//            LOG.error("", e);
//        }
	}

	private static void createOrUpdate(SimpleEntity entity, String bagName) {
		try {
			DBSession session = new LocalDBSession();
			EntityBag bag = session.createOrGetBag(bagName);

			boolean exists = false;

			try {
				SimpleEntity e = bag.getEntity(entity.getInternalId());
				if(e != null) {
					exists = true;
				}
			} catch(ResourceNotFoundException e) {

			}

			if(exists) {
				LOG.debug("Entity already exists, updating: {}", entity);
				bag.updateEntity(entity);
			} else {
				LOG.debug("Entity does not yet exist, creating: {}", entity);
				bag.addEntity(entity);
			}
		} catch (JasDBStorageException e) {
			LOG.error("", e);
		}
	}

}
