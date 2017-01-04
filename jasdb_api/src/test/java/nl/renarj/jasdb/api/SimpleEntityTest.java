/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import nl.renarj.jasdb.api.properties.*;
import nl.renarj.jasdb.core.exceptions.MetadataParseException;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

/**
 * User: renarj
 * Date: 2/10/12
 * Time: 2:22 PM
 */
public class SimpleEntityTest {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleEntityTest.class);

    @Test
    public void testSerializeDeserialize() throws MetadataParseException {
        SimpleEntity entity = new SimpleEntity(UUID.randomUUID().toString());
        entity.addProperty("test1", "value1");
        entity.addProperty("test1", "value2");
        entity.addProperty("test2", "someValue");
        entity.addProperty("test3", 1L);
        entity.addProperty("test3", 2L);
        entity.addProperty("test4", 1L);
        entity.addProperty("booleanValueFalse", false);
        entity.addProperty("booleanValueTrue", true);
        entity.addProperty("booleanMultiValue", true);
        entity.addProperty("booleanMultiValue", true);
        entity.addProperty("booleanMultiValue", false);
        entity.addProperty("embeddedJson", "{\"embeddedJsonField\":\"embeddedText\"}");
        assertEntity(entity);

        String serializedEntity = SimpleEntity.toJson(entity);
        LOG.debug(serializedEntity);

        SimpleEntity deserializedEntity = SimpleEntity.fromJson(serializedEntity);
        assertEntity(deserializedEntity);
    }

    @Test
    public void testEmbeddedEntity() throws MetadataParseException {
        SimpleEntity entity = new SimpleEntity("SomeId");
        entity.setProperty("simpleProperty1", 100L);
        entity.setProperty("multiValueProperty", "value1", "value2", "value3");
        entity.setProperty("integerProperty", 200);

        EmbeddedEntity embeddedEntity = new EmbeddedEntity();
        embeddedEntity.setProperty("embeddedProperty1", 50l, 60l, 70l);
        embeddedEntity.setProperty("embeddedString", "simpleStringValue");
        embeddedEntity.setProperty("embeddedMultivalue", "emValue1", "emValue2", "emValue3");
        entity.addEntity("embedded", embeddedEntity);

        String serializedEntity = SimpleEntity.toJson(entity);
        LOG.debug(serializedEntity);
        SimpleEntity deserializedEntity = SimpleEntity.fromJson(serializedEntity);

        assertTrue(deserializedEntity.hasProperty("simpleProperty1"));
        assertTrue(deserializedEntity.hasProperty("multiValueProperty"));
        assertTrue(deserializedEntity.hasProperty("integerProperty"));
        assertEquals(100, (long)deserializedEntity.getProperty("simpleProperty1").getFirstValueObject());
        assertEquals(3, deserializedEntity.getProperty("multiValueProperty").getValues().size());
        assertEquals("value1", deserializedEntity.getProperty("multiValueProperty").getValues().get(0).getValue());
        assertEquals("value2", deserializedEntity.getProperty("multiValueProperty").getValues().get(1).getValue());
        assertEquals("value3", deserializedEntity.getProperty("multiValueProperty").getValues().get(2).getValue());
        assertEquals(200L, (long)deserializedEntity.getProperty("integerProperty").getFirstValueObject());

        /* Test embedded property retrieval */
        assertNotNull(deserializedEntity.getProperty("embedded.embeddedProperty1"));
        assertThat(deserializedEntity.getProperty("embedded.embeddedProperty1").getFirstValue().toString(), is("50"));

        assertThat(deserializedEntity.getValues("embedded.embeddedMultivalue"), hasItems());
        assertThat((String)deserializedEntity.getValue("embedded.embeddedString"), is("simpleStringValue"));

        assertTrue("There should be an embedded entity", deserializedEntity.hasProperty("embedded"));
        Object embedded = deserializedEntity.getProperty("embedded").getFirstValueObject();
        assertNotNull(embedded);
        assertEquals("Embedded entity should be of type SimpleEntity", EmbeddedEntity.class, embedded.getClass());

        embeddedEntity = (EmbeddedEntity)embedded;
        assertTrue(embeddedEntity.hasProperty("embeddedProperty1"));
        assertTrue(embeddedEntity.hasProperty("embeddedString"));
        assertTrue(embeddedEntity.hasProperty("embeddedMultivalue"));
        assertEquals(50L, embeddedEntity.getProperty("embeddedProperty1").getValues().get(0).getValue());
        assertEquals(60L, embeddedEntity.getProperty("embeddedProperty1").getValues().get(1).getValue());
        assertEquals(70L, embeddedEntity.getProperty("embeddedProperty1").getValues().get(2).getValue());
        assertEquals("simpleStringValue", embeddedEntity.getProperty("embeddedString").getFirstValueObject());
        assertEquals("emValue1", embeddedEntity.getProperty("embeddedMultivalue").getValues().get(0).getValue());
        assertEquals("emValue2", embeddedEntity.getProperty("embeddedMultivalue").getValues().get(1).getValue());
        assertEquals("emValue3", embeddedEntity.getProperty("embeddedMultivalue").getValues().get(2).getValue());
    }

    @Test
    public void testMultiValueEmbedded() throws MetadataParseException {
        SimpleEntity entity = new SimpleEntity();

        EmbeddedEntity embeddedEntity1 = new EmbeddedEntity();
        embeddedEntity1.addProperty("username", "user1");
        embeddedEntity1.addProperty("grant", "rw");

        EmbeddedEntity embeddedEntity2 = new EmbeddedEntity();
        embeddedEntity2.addProperty("username", "user2");
        embeddedEntity2.addProperty("grant", "r");

        entity.addEntity("grants", embeddedEntity1);
        entity.addEntity("grants", embeddedEntity2);

        String multiEmbeddedJson = SimpleEntity.toJson(entity);

        entity = SimpleEntity.fromJson(multiEmbeddedJson);
        Property property = entity.getProperty("grants");

        assertThat(property.getValues().size(), is(2));
        List<Value> values = property.getValues();
        EntityValue entityValue = (EntityValue) values.get(0);
        assertThat(entityValue.toEntity().getValue("username").toString(), is("user1"));
        assertThat(entityValue.toEntity().getValue("grant").toString(), is("rw"));

        entityValue = (EntityValue) values.get(1);
        assertThat(entityValue.toEntity().getValue("username").toString(), is("user2"));
        assertThat(entityValue.toEntity().getValue("grant").toString(), is("r"));
    }

    @Test
    public void testEntityUpdate() {
        SimpleEntity entity = new SimpleEntity();
        entity.setProperty("field", "value");
        entity.addProperty("field", "value2");
        
        Property prop = entity.getProperty("field");
        Assert.assertNotNull(prop);
        Assert.assertEquals("There should be two values", 2, prop.getValues().size());
        Assert.assertEquals("First value should be 'value'", "value", prop.getValues().get(0).toString());
        Assert.assertEquals("Second value should be 'value2'", "value2", prop.getValues().get(1).toString());

        prop.removeValue(new StringValue("value"));
        Assert.assertEquals("There should be one remaining value", 1, prop.getValues().size());
        Assert.assertEquals("Value should be 'value2'", "value2", prop.getValues().get(0).toString());
        
        
        prop.addValue(new StringValue("value3"));
        prop.addValue(new StringValue("value4"));
        prop.addValue(new StringValue("value5"));
        Assert.assertEquals("There should be two values", 4, prop.getValues().size());
        entity.setProperty("field", "replacedValue");

        prop = entity.getProperty("field");
        Assert.assertEquals("There should be one remaining value", 1, prop.getValues().size());
        Assert.assertEquals("Value should be 'value2'", "replacedValue", prop.getValues().get(0).toString());
    }

    @Test
    public void testSerializeDeserializeEmptyProperty() throws MetadataParseException {
        SimpleEntity entity = new SimpleEntity();
        Property property = entity.createProperty("emptyProperty");
        assertEquals("emptyProperty", property.getPropertyName());

        String serializedEntity = SimpleEntity.toJson(entity);
        LOG.info(serializedEntity);
        entity = SimpleEntity.fromJson(serializedEntity);
        assertFalse(entity.hasProperty("emptyProperty"));
    }

    @Test
    public void deserializeEmbeddedEntity() throws MetadataParseException {
        String entityString = "{\"properties\":{\"__ID\":null,\"embedded\":{\"properties\":{\"embeddedMultivalue\":[\"emValue1\",\"emValue2\",\"emValue3\"],\"embeddedProperty1\":[50,60,70],\"embeddedString\":\"simpleStringValue\"}},\"integerProperty\":200,\"multiValueProperty\":[\"value1\",\"value2\",\"value3\"],\"simpleProperty1\":100}}";
        SimpleEntity entity = SimpleEntity.fromJson(entityString);
        assertNotNull(entity);
    }

    @Test
    public void deserializeEntityNoProperties() throws MetadataParseException {
        String entityString = "{\"city\":[\"Amsterdam\",\"Rotterdam\"],\"someProperty\":\"MyValue\"}";
        SimpleEntity entity = SimpleEntity.fromJson(entityString);
        Property property1 = entity.getProperty("city");
        Property property2 = entity.getProperty("someProperty");
        Assert.assertNotNull("Property should be present", property1);
        Assert.assertNotNull("Property should be present", property2);
        Assert.assertEquals("Unexpected nr. of values", 2, property1.getValues().size());
        Assert.assertEquals("Unexpected nr. of values", 1, property2.getValues().size());

        Value property1Value1 = property1.getValues().get(0);
        Assert.assertNotNull("Value should be present", property1Value1);
        Assert.assertEquals("Value is unexpected", "Amsterdam", property1Value1.getValue());
        Value property1Value2 = property1.getValues().get(1);
        Assert.assertNotNull("Value should be present", property1Value2);
        Assert.assertEquals("Value is unexpected", "Rotterdam", property1Value2.getValue());

        Value property2Value1 = property2.getValues().get(0);
        Assert.assertNotNull("Value should be present", property2Value1);
        Assert.assertEquals("Value is unexpected", "MyValue", property2Value1.getValue());
    }

    @Test
    public void testSetBulkProperties() {
        SimpleEntity simpleEntity = new SimpleEntity();

        Map<String, Object> properties = new HashMap<>();
        properties.put("field1", 100);
        properties.put("field2", 300l);
        properties.put("field3", "Simple String");

        Calendar calendar = Calendar.getInstance();
        calendar.set(2013, 1, 31, 0, 0, 0);
        String expectedTime = calendar.getTime().toString();
        properties.put("field4", calendar.getTime());

        simpleEntity.setProperties(properties);

        assertNotNull(simpleEntity.getProperty("field1"));
        assertNotNull(simpleEntity.getProperty("field2"));
        assertNotNull(simpleEntity.getProperty("field3"));
        assertNotNull(simpleEntity.getProperty("field4"));

        assertEquals(100, (int)simpleEntity.getProperty("field1").getFirstValueObject());
        assertEquals(300L, (long)simpleEntity.getProperty("field2").getFirstValueObject());
        assertEquals("Simple String", simpleEntity.getProperty("field3").getFirstValueObject());
        assertEquals(expectedTime, simpleEntity.getProperty("field4").getFirstValueObject());
    }

    @Test
    public void testRetainArray() throws JsonProcessingException, MetadataParseException {
        ObjectMapper objectMapper = new ObjectMapper();

        Person person = new Person();
        person.setName("Person Test");
        person.setPhones(Collections.singletonList("12345678"));

        String json = objectMapper.writeValueAsString(person);
        SimpleEntity simpleEntity = SimpleEntity.fromJson(json);
        assertThat(SimpleEntity.toJson(simpleEntity), containsString("\"phones\":[\"12345678\"]"));
    }

    private void assertEntity(SimpleEntity entity) {
        assertThat(entity.hasProperty("test1"), is(true));
        assertThat(entity.hasProperty("test2"), is(true));
        assertThat(entity.hasProperty("test3"), is(true));
        assertThat(entity.hasProperty("test4"), is(true));
        assertThat(entity.hasProperty("booleanValueFalse"), is(true));
        assertThat(entity.hasProperty("booleanValueTrue"), is(true));
        assertThat(entity.hasProperty("booleanMultiValue"), is(true));
        assertThat(entity.hasProperty("embeddedJson"), is(true));
        //ensure the embedded raw json data is not part of the entity
        assertThat(entity.hasProperty("embeddedJsonField"), is(false));

        Property property1 = entity.getProperty("test1");
        Property property2 = entity.getProperty("test2");
        Property property3 = entity.getProperty("test3");
        Property property4 = entity.getProperty("test4");

        assertThat(property1.getValues().size(), is(2));
        assertThat(property2.getValues().size(), is(1));
        assertThat(property3.getValues().size(), is(2));
        assertThat(property4.getValues().size(), is(1));

        assertThat(entity.getProperty("booleanValueFalse").getValues(), hasItems((Value) new BooleanValue(false)));
        assertThat(entity.getProperty("booleanValueTrue").getValues(), hasItems((Value)new BooleanValue(true)));
        assertThat(entity.getProperty("booleanMultiValue").getValues(), hasItems((Value)new BooleanValue(true), new BooleanValue(true), new BooleanValue(false)));


        assertThat(entity.getProperty("test1").getValues(), hasItems((Value) new StringValue("value1"), new StringValue("value2")));
        assertThat(entity.getProperty("test2").getValues(), hasItems((Value) new StringValue("someValue")));
        assertThat(entity.getProperty("test3").getValues(), hasItems((Value) new LongValue(1l), new LongValue(2l)));
        assertThat(entity.getProperty("test4").getValues(), hasItems((Value)new LongValue(1l)));

        assertThat(entity.getProperty("embeddedJson").getFirstValueObject(), is("{\"embeddedJsonField\":\"embeddedText\"}"));
    }

    private class Person {
        private String name;
        private List<String> phones;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public List<String> getPhones() {
            return phones;
        }

        public void setPhones(List<String> phones) {
            this.phones = phones;
        }
    }
}
