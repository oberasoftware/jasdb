package com.oberasoftware.jasdb.entitymapper;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.oberasoftware.jasdb.api.entitymapper.MapResult;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.session.Entity;
import com.oberasoftware.jasdb.api.session.Property;
import com.oberasoftware.jasdb.api.session.Value;
import com.oberasoftware.jasdb.core.EmbeddedEntity;
import com.oberasoftware.jasdb.core.SimpleEntity;
import com.oberasoftware.jasdb.core.properties.EntityValue;
import com.oberasoftware.jasdb.core.properties.MultivalueProperty;
import org.junit.Test;
import org.slf4j.Logger;

import java.util.List;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author Renze de Vries
 */
public class EntityMapperTest {
    private static final Logger LOG = getLogger(EntityMapperTest.class);

    private static final String FIELD_VALUE1 = "I have a value here";
    private static final String FIELD_VALUE2 = "And another value there";
    private static final long LONG_VALUE = 10001l;
    private static final String SOME_EMAIL = "test@test.com";
    private static final String SOME_FIELD = "someField";
    private static final String ANOTHER_NUMBER_FIELD = "anotherNumberField";
    private static final String DIFFERENT_NAME_THAN_FIELD = "differentNameThanField";
    private static final String SOME_VALUE_1 = "someValue1";
    private static final String SOME_VALUE_8 = "someValue8";
    private static final String FIELD_4 = "field4";
    private static final String FIELD_0 = "field0";
    private static final String PROPERTIES = "properties";
    private static final String MY_NAME = "MyName";
    private static final String ITEMS = "ITEMS";
    private static final String NAME = "name";
    private static final String CUSTOM_KEY = "customKey";
    private static final String TEST_1 = "test1";
    private static final String TEST_3 = "test3";
    private static final String TEST_0 = "test0";
    private static final String EMAIL_ADDRESS = "emailAddress";
    private static final String CUSTOM_ENUM = "customEnum";
    private static final String SOME_BOOLEAN_FIELD = "someBoolean";

    @Test
    public void testMapToSimpleEntity() throws JasDBStorageException {
        AnnotationEntityMapper mapper = new AnnotationEntityMapper();

        BasicEntity basicEntity = new BasicEntity(FIELD_VALUE1, 10001, FIELD_VALUE2);
        MapResult result = mapper.mapTo(basicEntity);

        assertThat(result.getBagName(), is("MyBAG"));
        Entity entity = result.getJasDBEntity();

        List<Property> properties = entity.getProperties();
        assertThat(properties.size(), is(5));

        assertThat(entity.getValue(SOME_FIELD), is(FIELD_VALUE1));
        assertThat(entity.getValue(ANOTHER_NUMBER_FIELD), is(LONG_VALUE));
        assertThat(entity.getValue(DIFFERENT_NAME_THAN_FIELD), is(FIELD_VALUE2));

        LOG.debug(SimpleEntity.toJson(entity));
    }

    @Test
    public void testMapToBasicObject() throws JasDBStorageException {
        AnnotationEntityMapper mapper = new AnnotationEntityMapper();

        SimpleEntity entity = new SimpleEntity();
        entity.addProperty(SOME_FIELD, FIELD_VALUE1);
        entity.addProperty(ANOTHER_NUMBER_FIELD, LONG_VALUE);
        entity.addProperty(DIFFERENT_NAME_THAN_FIELD, FIELD_VALUE2);
        entity.addProperty(SOME_BOOLEAN_FIELD, true);

        BasicEntity basicEntity = mapper.mapFrom(BasicEntity.class, entity);
        assertThat(basicEntity.getSomeField(), is(FIELD_VALUE1));
        assertThat(basicEntity.getAnotherNumberField(), is(LONG_VALUE));
        assertThat(basicEntity.getJustSomeTextField(), is(FIELD_VALUE2));
        assertThat(basicEntity.isSomeBoolean(), is(true));
    }

    @Test
    public void testMapToSimpleEntityWithCollections() throws JasDBStorageException {
        AnnotationEntityMapper mapper = new AnnotationEntityMapper();

        String keyField = UUID.randomUUID().toString();

        ComplexEntity complexEntity = new ComplexEntity(Lists.newArrayList(TEST_1, TEST_3, TEST_0), SOME_EMAIL,
                ComplexEntity.CUSTOM_ENUM.VALUE2, MY_NAME, keyField,
                new ImmutableMap.Builder<String, String>()
                .put(FIELD_4, SOME_VALUE_1)
                .put(FIELD_0, SOME_VALUE_8).build());
        complexEntity.setRelatedEntities(Lists.newArrayList(new BasicEntity("testField1", 0001l, "justAField1"), new BasicEntity("testField2", 0002l, "justAField2")));

        MapResult result = mapper.mapTo(complexEntity);
        assertThat(result.getBagName(), is("COMPLEX_TEST"));

        Entity entity = result.getJasDBEntity();
        assertThat(entity.getValue(NAME), is(MY_NAME));
        assertThat(entity.getValue(CUSTOM_KEY), is(keyField));
        assertThat(entity.getValue(EMAIL_ADDRESS), is(SOME_EMAIL));
        assertThat(entity.getValue(CUSTOM_ENUM), is(ComplexEntity.CUSTOM_ENUM.VALUE2.name()));
        assertThat(entity.getInternalId(), is(keyField));

        Entity propertiesEntity = entity.getEntity(PROPERTIES);
        assertThat(propertiesEntity.getValue(FIELD_4), is(SOME_VALUE_1));
        assertThat(propertiesEntity.getValue(FIELD_0), is(SOME_VALUE_8));

        List<String> values = entity.getValues(ITEMS);
        assertThat(values, hasItems(TEST_1, TEST_3, TEST_0));

        List<Value> relatedEntities = entity.getProperty("relatedEntities").getValues();
        assertThat(relatedEntities.size(), is(2));
        var entity1 = ((EntityValue)relatedEntities.get(0)).getValue();
        assertThat(entity1, notNullValue());
        assertThat(entity1.getProperty("someField").getFirstValueObject(), is("testField1"));
        assertThat(entity1.getValue("anotherNumberField"), is(1l));
        assertThat(entity1.getValue("differentNameThanField"), is("justAField1"));

        var entity2 = ((EntityValue)relatedEntities.get(1)).getValue();
        assertThat(entity2, notNullValue());
        assertThat(entity2.getProperty("someField").getFirstValueObject(), is("testField2"));
        assertThat(entity2.getValue("anotherNumberField"), is(2l));
        assertThat(entity2.getValue("differentNameThanField"), is("justAField2"));

        LOG.info(SimpleEntity.toJson(entity));
    }

    @Test
    public void testMapSimpleEntityToComplexType() throws JasDBStorageException {
        AnnotationEntityMapper mapper = new AnnotationEntityMapper();

        String keyField = UUID.randomUUID().toString();

        SimpleEntity entity = new SimpleEntity(keyField);
        entity.addProperty(NAME, MY_NAME);
        entity.addProperty(CUSTOM_KEY, keyField);
        entity.addProperty(ITEMS, TEST_1, TEST_3, TEST_0);
        entity.addProperty(EMAIL_ADDRESS, SOME_EMAIL);
        entity.addProperty(CUSTOM_ENUM, ComplexEntity.CUSTOM_ENUM.VALUE1.name());

        EmbeddedEntity propertiesEntity = new EmbeddedEntity();
        entity.addEntity(PROPERTIES, propertiesEntity);
        propertiesEntity.addProperty(FIELD_4, SOME_VALUE_1);
        propertiesEntity.addProperty(FIELD_0, SOME_VALUE_8);

        EmbeddedEntity basicEntityEmbedded = new EmbeddedEntity();
        basicEntityEmbedded.setProperty("someField", "thisIsSomeField");
        basicEntityEmbedded.setProperty("anotherNumberField", 9999333);
        basicEntityEmbedded.setProperty("differentNameThanField", "bladiebla");
        basicEntityEmbedded.addProperty("someBoolean", true);
        entity.addEntity("basicEntity", basicEntityEmbedded);

        EmbeddedEntity basicListEntityEmbedded1 = new EmbeddedEntity();
        basicListEntityEmbedded1.setProperty("someField", "thisIsSomeField1");
        basicListEntityEmbedded1.setProperty("anotherNumberField", 9999331);
        basicListEntityEmbedded1.addProperty("someBoolean", false);
        basicListEntityEmbedded1.setProperty("differentNameThanField", "bladiebla1");

        EmbeddedEntity basicListEntityEmbedded2 = new EmbeddedEntity();
        basicListEntityEmbedded2.setProperty("someField", "thisIsSomeField2");
        basicListEntityEmbedded2.setProperty("anotherNumberField", 9999332);
        basicListEntityEmbedded2.setProperty("differentNameThanField", "bladiebla2");
        basicListEntityEmbedded2.addProperty("someBoolean", true);
        entity.addProperty(new MultivalueProperty("relatedEntities", true).addValue(new EntityValue(basicListEntityEmbedded1)).addValue(new EntityValue(basicListEntityEmbedded2)));

        ComplexEntity complexEntity = mapper.mapFrom(ComplexEntity.class, entity);
        assertThat(complexEntity.getCustomKey(), is(keyField));
        assertThat(complexEntity.getName(), is(MY_NAME));
        assertThat(complexEntity.getEmailAddress(), is(SOME_EMAIL));
        assertThat(complexEntity.getCustomEnum(), is(ComplexEntity.CUSTOM_ENUM.VALUE1));
        assertThat(complexEntity.getRelatedItems(), hasItems(TEST_1, TEST_3, TEST_0));

        assertThat(complexEntity.getProperties().size(), is(2));
        assertThat(complexEntity.getProperties().get(FIELD_4), is(SOME_VALUE_1));
        assertThat(complexEntity.getProperties().get(FIELD_0), is(SOME_VALUE_8));

        assertThat(complexEntity.getBasicEntity(), notNullValue());
        assertThat(complexEntity.getBasicEntity().getSomeField(), is("thisIsSomeField"));
        assertThat(complexEntity.getBasicEntity().getAnotherNumberField(), is(9999333L));
        assertThat(complexEntity.getBasicEntity().getJustSomeTextField(), is("bladiebla"));

        assertThat(complexEntity.getRelatedEntities().size(), is(2));
        assertThat(complexEntity.getRelatedEntities().get(0), notNullValue());
        assertThat(complexEntity.getRelatedEntities().get(1), notNullValue());
        assertThat(complexEntity.getRelatedEntities().get(0).getSomeField(), is("thisIsSomeField1"));
        assertThat(complexEntity.getRelatedEntities().get(0).getAnotherNumberField(), is(9999331l));
        assertThat(complexEntity.getRelatedEntities().get(0).getJustSomeTextField(), is("bladiebla1"));
        assertThat(complexEntity.getRelatedEntities().get(1).getSomeField(), is("thisIsSomeField2"));
        assertThat(complexEntity.getRelatedEntities().get(1).getAnotherNumberField(), is(9999332l));
        assertThat(complexEntity.getRelatedEntities().get(1).getJustSomeTextField(), is("bladiebla2"));

    }

    @Test
    public void testCollectionSingleEntry() throws JasDBStorageException {
        AnnotationEntityMapper mapper = new AnnotationEntityMapper();
        String keyField = UUID.randomUUID().toString();

        ComplexEntity complexEntity = new ComplexEntity(Lists.newArrayList(TEST_1), SOME_EMAIL,
                ComplexEntity.CUSTOM_ENUM.VALUE2, MY_NAME, keyField,
                new ImmutableMap.Builder<String, String>()
                        .put(FIELD_0, SOME_VALUE_8).build());
        MapResult result = mapper.mapTo(complexEntity);
        assertThat(result.getBagName(), is("COMPLEX_TEST"));

        Entity entity = result.getJasDBEntity();
        List<String> values = entity.getValues(ITEMS);
        assertThat(values, hasItems(TEST_1));

        String json = SimpleEntity.toJson(entity);
        LOG.debug("Json: {}", json);

        complexEntity = mapper.mapFrom(ComplexEntity.class, SimpleEntity.fromJson(json));
        assertThat(complexEntity.getRelatedItems().size(), is(1));
    }

    @Test
    public void testEmbeddedEntity() throws JasDBStorageException {
        AnnotationEntityMapper mapper = new AnnotationEntityMapper();
        String keyField = UUID.randomUUID().toString();

        ComplexEntity complexEntity = new ComplexEntity(Lists.newArrayList(TEST_1), SOME_EMAIL,
                ComplexEntity.CUSTOM_ENUM.VALUE2, MY_NAME, keyField,
                new ImmutableMap.Builder<String, String>()
                        .put(FIELD_0, SOME_VALUE_8).build());
        complexEntity.setBasicEntity(new BasicEntity("testField", 9933322, "justSomeText"));

        var entity = mapper.mapTo(complexEntity).getJasDBEntity();
        assertThat(entity.getEntity("basicEntity"), notNullValue());
    }

    @Test
    public void testLoadLocomotive() throws JasDBStorageException {
        var loc = SimpleEntity.fromJson("        {\n" +
                "            \"__ID\": \"00001ec0-3194-1873-0000-01878b7e4ab7\",\n" +
                "            \"attributes\": {},\n" +
                "            \"controllerId\": \"test\",\n" +
                "            \"locAddress\": 99,\n" +
                "            \"name\": \"DB 103\",\n" +
                "            \"thingId\": \"db103\"\n" +
                "        }");

        AnnotationEntityMapper mapper = new AnnotationEntityMapper();
        Locomotive mappedResult = mapper.mapFrom(Locomotive.class, loc);
        assertThat(mappedResult, notNullValue());
    }

    @Test
    public void testNullKey() throws JasDBStorageException {
        var loc = new Locomotive(99, "test", "thing1", "blaaat");
        AnnotationEntityMapper entityMapper = new AnnotationEntityMapper();
        var entity = entityMapper.mapTo(loc).getJasDBEntity();
        assertThat(entity, notNullValue());
        assertThat(entity.getInternalId(), nullValue());

        entity.setInternalId("fakeKey");

        var remapped = entityMapper.mapFrom(Locomotive.class, entity);
        assertThat(remapped.getId(), notNullValue());
        assertThat(remapped.getId(), is("fakeKey"));

    }
}
