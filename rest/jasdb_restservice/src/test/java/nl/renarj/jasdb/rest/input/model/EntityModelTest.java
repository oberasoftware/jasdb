package nl.renarj.jasdb.rest.input.model;

import junit.framework.Assert;
import nl.renarj.jasdb.SimpleBaseTest;
import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.api.query.QueryResult;
import nl.renarj.jasdb.core.exceptions.JasDBException;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.platform.HomeLocatorUtil;
import nl.renarj.jasdb.rest.input.InputElement;
import nl.renarj.jasdb.rest.input.OrderParam;
import nl.renarj.jasdb.rest.input.conditions.FieldCondition;
import nl.renarj.jasdb.rest.loaders.EntityModelLoader;
import nl.renarj.jasdb.rest.model.RestBag;
import nl.renarj.jasdb.rest.model.RestEntity;
import nl.renarj.jasdb.rest.model.streaming.StreamableEntityCollection;
import nl.renarj.jasdb.rest.serializers.json.JsonRestResponseHandler;
import org.junit.After;
import org.junit.Test;

import java.util.ArrayList;

public class EntityModelTest extends SimpleBaseTest {

	@After
	public void tearDown() throws JasDBException {
		super.tearDown();
        cleanData();
	}

    @Test
    public void before() throws JasDBStorageException {
        System.setProperty(HomeLocatorUtil.JASDB_HOME, SimpleBaseTest.tmpDir.toString());
    }
	
	@Test
	public void testInsertEntity() throws Exception {
		EntityModelLoader eml = new EntityModelLoader();
		
		InputElement bagElement = new InputElement("Bags").setCondition(new FieldCondition(FieldCondition.ID_PARAM, "bag1"));
		bagElement.setResult(new RestBag("default", "bag1", 0, 0));
		InputElement entityElement = new InputElement("Entities");
		entityElement.setPrevious(bagElement);
		
		eml.writeEntry(entityElement, new JsonRestResponseHandler(), "{\"__ID\":\"\", \"properties\": {\"age\": \"80\", \"somefield\" : \"somevalue\"}}", null);

		entityElement.setCondition(new FieldCondition("age", "80"));
		RestEntity foundEntities = eml.loadModel(entityElement, null, "10", new ArrayList<OrderParam>(), null);
		Assert.assertNotNull("There should be a result", foundEntities);
		Assert.assertTrue("Unexpected rest entities", foundEntities instanceof StreamableEntityCollection);
		StreamableEntityCollection entityCollection = (StreamableEntityCollection) foundEntities;
		Assert.assertEquals("There should be one entity in the collection", 1, entityCollection.getResult().size());

        QueryResult q = entityCollection.getResult();
        SimpleEntity entity = q.next();
		Assert.assertNotNull("Entity should not be null", entity);
		Assert.assertEquals("There should be two properties", 3, entity.getProperties().size());

        Assert.assertEquals("There should be one value for somefield", 1, entity.getProperty("somefield").getValues().size());
		Assert.assertEquals("Field 'somefield' should have value 'somevalue'", "somevalue", entity.getProperty("somefield").getValues().get(0).getValue());
		Assert.assertEquals("Field 'age' should have value '80'", "80", entity.getProperty("age").getValues().get(0).getValue());
	}
}
