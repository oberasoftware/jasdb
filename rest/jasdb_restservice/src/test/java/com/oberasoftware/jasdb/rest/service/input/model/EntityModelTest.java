package com.oberasoftware.jasdb.rest.service.input.model;

import com.oberasoftware.jasdb.engine.StorageService;
import com.oberasoftware.jasdb.engine.StorageServiceFactory;
import com.oberasoftware.jasdb.rest.service.loaders.EntityModelLoader;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

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
//        when(storageServiceFactory.getOrCreateStorageService(TEST_INSTANCE, TEST_BAGNAME)).thenReturn(storageService);
//
//		InputElement bagElement = new InputElement("Bags").setCondition(new FieldCondition(FieldCondition.ID_PARAM, TEST_BAGNAME));
//		bagElement.setResult(new RestBag(TEST_INSTANCE, TEST_BAGNAME, 0, 0));
//		InputElement entityElement = new InputElement("Entities");
//		entityElement.setPrevious(bagElement);
//
//        entityModelLoader.writeEntry(entityElement, new JsonRestResponseHandler(), "{\"__ID\":\"\", \"age\": \"80\", \"somefield\" : \"somevalue\"}", null);
//        ArgumentCaptor<SimpleEntity> entityArgumentCaptor = ArgumentCaptor.forClass(SimpleEntity.class);
//        verify(storageService, times(1)).insertEntity(any(RequestContext.class), entityArgumentCaptor.capture());
//
//        SimpleEntity insertedEntity = entityArgumentCaptor.getValue();
//        assertThat(insertedEntity.hasProperty("age"), is(true));
//        assertThat(insertedEntity.hasProperty("somefield"), is(true));
//        assertThat(insertedEntity.getValue("age"), is("80"));
//        assertThat(insertedEntity.getValue("somefield"), is("somevalue"));
	}

    @Test
    public void testFindEntities() throws Exception {
//        QueryResult queryResult = mock(QueryResult.class);
//        when(storageServiceFactory.getOrCreateStorageService(TEST_INSTANCE, TEST_BAGNAME)).thenReturn(storageService);
//        when(storageService.search(any(RequestContext.class), any(BlockOperation.class), any(SearchLimit.class), anyListOf(SortParameter.class))).thenReturn(queryResult);
//
//        InputElement bagElement = new InputElement("Bags").setCondition(new FieldCondition(FieldCondition.ID_PARAM, TEST_BAGNAME));
//        bagElement.setResult(new RestBag(TEST_INSTANCE, TEST_BAGNAME, 0, 0));
//        InputElement entityElement = new InputElement("Entities");
//        entityElement.setPrevious(bagElement);
//
//        entityElement.setCondition(new FieldCondition("age", "80"));
//        RestEntity foundEntities = entityModelLoader.loadModel(entityElement, null, "10", new ArrayList<>(), null);
//        assertThat(foundEntities, notNullValue());
//        assertThat(((StreamableEntityCollection)foundEntities).getResult(), is(queryResult));
//
//        ArgumentCaptor<BlockOperation> blockOperationArgumentCaptor = ArgumentCaptor.forClass(BlockOperation.class);
//        ArgumentCaptor<SearchLimit> searchLimitArgumentCaptor = ArgumentCaptor.forClass(SearchLimit.class);
//        verify(storageService, times(1)).search(any(RequestContext.class), blockOperationArgumentCaptor.capture(), searchLimitArgumentCaptor.capture(), anyListOf(SortParameter.class));
//
//        BlockOperation blockOperation = blockOperationArgumentCaptor.getValue();
//        assertThat(blockOperation.hasConditions("age"), is(true));
//
//        SearchCondition searchCondition = blockOperation.getConditions("age").iterator().next();
//        assertThat(searchCondition.getClass().toString(), is(EqualsCondition.class.toString()));
//        assertThat(((EqualsCondition)searchCondition).getKey(), is(new StringKey("80")));
//
//        assertThat(searchLimitArgumentCaptor.getValue().getBegin(), is(0));
//        assertThat(searchLimitArgumentCaptor.getValue().getMax(), is(10));
    }
}
