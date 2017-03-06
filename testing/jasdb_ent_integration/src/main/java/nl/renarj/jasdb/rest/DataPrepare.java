package nl.renarj.jasdb.rest;

import com.oberasoftware.jasdb.service.local.LocalDBSession;
import nl.renarj.jasdb.SimpleBaseTest;
import nl.renarj.jasdb.api.DBSession;
import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.api.model.EntityBag;
import nl.renarj.jasdb.core.SimpleKernel;
import nl.renarj.jasdb.core.exceptions.JasDBException;
import nl.renarj.jasdb.index.keys.types.LongKeyType;
import nl.renarj.jasdb.index.keys.types.StringKeyType;
import nl.renarj.jasdb.index.search.IndexField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

public class DataPrepare {
	private static final int NUMBER_BAGS = 1;
	private static final int NUMBER_ENTITIES = 1000;
	
	private static Logger LOG = LoggerFactory.getLogger(DataPrepare.class);
	
	public static void doDataPrepare() {
		try {
			SimpleBaseTest.cleanData();
			try {
				LOG.info("Starting generating test data {} bags with {} entities", NUMBER_BAGS, NUMBER_ENTITIES);
				Random rnd = new Random(System.currentTimeMillis());
				DBSession dbSession = new LocalDBSession();
				for(int i=0; i<NUMBER_BAGS; i++) {
					Map<String, Integer> ageAmounts = new HashMap<>();
					
					EntityBag bag = dbSession.createOrGetBag("bag" + i);
					bag.ensureIndex(new IndexField("field1", new StringKeyType()), true);
					bag.ensureIndex(new IndexField("age", new LongKeyType()), false);
					
					int nrEnt = NUMBER_ENTITIES; //rnd.nextInt(NUMBER_ENTITIES);
					for(int j=0; j<nrEnt; j++) {
						long age = Long.valueOf(rnd.nextInt(120));
						
						SimpleEntity entity = new SimpleEntity();
						entity.addProperty("field1", "someValue" + j);
						entity.addProperty("age", age);
						entity.addProperty("myProperty", String.valueOf(rnd.nextInt(21394)));
						
						bag.addEntity(entity);
						
						String ageKey = String.valueOf(age);
						if(!ageAmounts.containsKey(String.valueOf(age))) {
							ageAmounts.put(ageKey, 0);
						}
						
						int amount = ageAmounts.get(ageKey);
						amount++;
						ageAmounts.put(ageKey, amount);
					}
					
					if(LOG.isDebugEnabled()) {
						for(Entry<String, Integer> entry : ageAmounts.entrySet()) {
							LOG.info("Data in Bag: {}, Key: {} amount: {}", new Object[] {"bag" + i, entry.getKey(), entry.getValue()});
						}
					}
				}
				LOG.info("Finished generating test data");
			} finally {
				LOG.info("Shutting down kernel");
				SimpleKernel.shutdown();
				LOG.info("Shutdown complete");
			}
		} catch(JasDBException e) {
			LOG.error("Unable to prepare data", e);
		}
	}
	
}
