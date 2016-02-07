package com.oberasoftware.jasdb.entitymapper;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.oberasoftware.jasdb.api.entitymapper.MapResult;
import nl.renarj.jasdb.api.EmbeddedEntity;
import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.api.properties.Property;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import org.junit.Test;
import org.slf4j.Logger;

import java.util.List;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
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

    @Test
    public void testMapToSimpleEntity() throws JasDBStorageException {
        AnnotationEntityMapper mapper = new AnnotationEntityMapper();

        BasicEntity basicEntity = new BasicEntity(FIELD_VALUE1, 10001, FIELD_VALUE2);
        MapResult result = mapper.mapTo(basicEntity);

        assertThat(result.getBagName(), is("MyBAG"));
        SimpleEntity entity = result.getJasDBEntity();

        List<Property> properties = entity.getProperties();
        assertThat(properties.size(), is(4));

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

        BasicEntity basicEntity = mapper.mapFrom(BasicEntity.class, entity);
        assertThat(basicEntity.getSomeField(), is(FIELD_VALUE1));
        assertThat(basicEntity.getAnotherNumberField(), is(LONG_VALUE));
        assertThat(basicEntity.getJustSomeTextField(), is(FIELD_VALUE2));
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

        MapResult result = mapper.mapTo(complexEntity);
        assertThat(result.getBagName(), is("COMPLEX_TEST"));

        SimpleEntity entity = result.getJasDBEntity();
        assertThat(entity.getValue(NAME), is(MY_NAME));
        assertThat(entity.getValue(CUSTOM_KEY), is(keyField));
        assertThat(entity.getValue(EMAIL_ADDRESS), is(SOME_EMAIL));
        assertThat(entity.getValue(CUSTOM_ENUM), is(ComplexEntity.CUSTOM_ENUM.VALUE2.name()));
        assertThat(entity.getInternalId(), is(keyField));

        SimpleEntity propertiesEntity = entity.getEntity(PROPERTIES);
        assertThat(propertiesEntity.getValue(FIELD_4), is(SOME_VALUE_1));
        assertThat(propertiesEntity.getValue(FIELD_0), is(SOME_VALUE_8));

        List<String> values = entity.getValues(ITEMS);
        assertThat(values, hasItems(TEST_1, TEST_3, TEST_0));
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

        ComplexEntity complexEntity = mapper.mapFrom(ComplexEntity.class, entity);
        assertThat(complexEntity.getCustomKey(), is(keyField));
        assertThat(complexEntity.getName(), is(MY_NAME));
        assertThat(complexEntity.getEmailAddress(), is(SOME_EMAIL));
        assertThat(complexEntity.getCustomEnum(), is(ComplexEntity.CUSTOM_ENUM.VALUE1));
        assertThat(complexEntity.getRelatedItems(), hasItems(TEST_1, TEST_3, TEST_0));

        assertThat(complexEntity.getProperties().size(), is(2));
        assertThat(complexEntity.getProperties().get(FIELD_4), is(SOME_VALUE_1));
        assertThat(complexEntity.getProperties().get(FIELD_0), is(SOME_VALUE_8));
    }
}
