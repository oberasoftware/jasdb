package nl.renarj.jasdb.rest.input.model;

import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.api.context.RequestContext;
import nl.renarj.jasdb.api.query.QueryResult;
import nl.renarj.jasdb.api.query.SortParameter;
import nl.renarj.jasdb.index.keys.impl.StringKey;
import nl.renarj.jasdb.index.result.SearchLimit;
import nl.renarj.jasdb.index.search.EqualsCondition;
import nl.renarj.jasdb.index.search.SearchCondition;
import nl.renarj.jasdb.rest.input.InputElement;
import nl.renarj.jasdb.rest.input.OrderParam;
import nl.renarj.jasdb.rest.input.conditions.FieldCondition;
import nl.renarj.jasdb.rest.loaders.EntityModelLoader;
import nl.renarj.jasdb.rest.model.RestBag;
import nl.renarj.jasdb.rest.model.RestEntity;
import nl.renarj.jasdb.rest.model.streaming.StreamableEntityCollection;
import nl.renarj.jasdb.rest.serializers.json.JsonRestResponseHandler;
import nl.renarj.jasdb.service.StorageService;
import nl.renarj.jasdb.service.StorageServiceFactory;
import nl.renarj.jasdb.storage.query.operators.BlockOperation;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.ArrayList;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class EntityModelTest {

    public static final String TEST_INSTANCE = "default";
    public static final String TEST_BAGNAME = "bag1";

    @Mock
    private StorageServiceFactory storageServiceFactory;

    @Mock
    private StorageService storageService;

    @InjectMocks
    private EntityModelLoader entityModelLoader;


	@Test
	public void testInsertEntity() throws Exception {
        when(storageServiceFactory.getOrCreateStorageService(TEST_INSTANCE, TEST_BAGNAME)).thenReturn(storageService);

		InputElement bagElement = new InputElement("Bags").setCondition(new FieldCondition(FieldCondition.ID_PARAM, TEST_BAGNAME));
		bagElement.setResult(new RestBag(TEST_INSTANCE, TEST_BAGNAME, 0, 0));
		InputElement entityElement = new InputElement("Entities");
		entityElement.setPrevious(bagElement);

        entityModelLoader.writeEntry(entityElement, new JsonRestResponseHandler(), "{\"__ID\":\"\", \"properties\": {\"age\": \"80\", \"somefield\" : \"somevalue\"}}", null);
        ArgumentCaptor<SimpleEntity> entityArgumentCaptor = ArgumentCaptor.forClass(SimpleEntity.class);
        verify(storageService, times(1)).insertEntity(any(RequestContext.class), entityArgumentCaptor.capture());

        SimpleEntity insertedEntity = entityArgumentCaptor.getValue();
        assertThat(insertedEntity.hasProperty("age"), is(true));
        assertThat(insertedEntity.hasProperty("somefield"), is(true));
        assertThat((String) insertedEntity.getValue("age"), is("80"));
        assertThat((String)insertedEntity.getValue("somefield"), is("somevalue"));
	}

    @Test
    public void testFindEntities() throws Exception {
        QueryResult queryResult = mock(QueryResult.class);
        when(storageServiceFactory.getOrCreateStorageService(TEST_INSTANCE, TEST_BAGNAME)).thenReturn(storageService);
        when(storageService.search(any(RequestContext.class), any(BlockOperation.class), any(SearchLimit.class), anyListOf(SortParameter.class))).thenReturn(queryResult);

        InputElement bagElement = new InputElement("Bags").setCondition(new FieldCondition(FieldCondition.ID_PARAM, TEST_BAGNAME));
        bagElement.setResult(new RestBag(TEST_INSTANCE, TEST_BAGNAME, 0, 0));
        InputElement entityElement = new InputElement("Entities");
        entityElement.setPrevious(bagElement);

        entityElement.setCondition(new FieldCondition("age", "80"));
        RestEntity foundEntities = entityModelLoader.loadModel(entityElement, null, "10", new ArrayList<OrderParam>(), null);
        assertThat(foundEntities, notNullValue());
        assertThat(((StreamableEntityCollection)foundEntities).getResult(), is(queryResult));

        ArgumentCaptor<BlockOperation> blockOperationArgumentCaptor = ArgumentCaptor.forClass(BlockOperation.class);
        ArgumentCaptor<SearchLimit> searchLimitArgumentCaptor = ArgumentCaptor.forClass(SearchLimit.class);
        verify(storageService, times(1)).search(any(RequestContext.class), blockOperationArgumentCaptor.capture(), searchLimitArgumentCaptor.capture(), anyListOf(SortParameter.class));

        BlockOperation blockOperation = blockOperationArgumentCaptor.getValue();
        assertThat(blockOperation.hasConditions("age"), is(true));

        SearchCondition searchCondition = blockOperation.getConditions("age").iterator().next();
        assertThat(searchCondition.getClass().toString(), is(EqualsCondition.class.toString()));
        assertThat((StringKey)((EqualsCondition)searchCondition).getKey(), is(new StringKey("80")));

        assertThat(searchLimitArgumentCaptor.getValue().getBegin(), is(0));
        assertThat(searchLimitArgumentCaptor.getValue().getMax(), is(10));
    }
}
