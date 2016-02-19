package nl.renarj.jasdb.rest.input;

import nl.renarj.jasdb.rest.exceptions.RestException;
import nl.renarj.jasdb.rest.exceptions.SyntaxException;
import nl.renarj.jasdb.rest.input.conditions.*;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

public class PathParserTest {
	private static final Logger LOG = LoggerFactory.getLogger(PathParserTest.class);
	
	@Test
	public void testRangeQuery() throws RestException {
		String testPath = "Bags(bag1)/Entities(age>10|age<12)";
		PathParser parser = new PathParser(testPath);
		List<InputElement> elements = getElements(parser);
		Assert.assertEquals("There should be two input elements", 2, elements.size());
		
		InputElement expectedBagElement = new InputElement("Bags").setCondition(new FieldCondition("ID", "bag1"));
		assertElement(expectedBagElement, elements.get(0));
		
		InputElement expectedEntityElement = new InputElement("Entities").setCondition(
				new OrBlockOperation()
					.addInputCondition(new FieldCondition("age", Operator.Larger_than, "10"))
					.addInputCondition(new FieldCondition("age", Operator.Smaller_than, "12"))
			);		
		assertElement(expectedEntityElement, elements.get(1));
	}
	
	@Test
	public void testAndOrPath() throws RestException {
		String testPath = "element(field1=value,field2=value|(field3=value|field4=value))";
		PathParser parser = new PathParser(testPath);
		List<InputElement> elements = getElements(parser);
		Assert.assertEquals("There should be one input elements", 1, elements.size());
		LOG.info("InputConditions: {}", elements.get(0).getCondition());
		
		InputElement expectedElement = new InputElement("element").setCondition(
				new OrBlockOperation()
					.addInputCondition(new AndBlockOperation()
						.addInputCondition(new FieldCondition("field1", "value"))
						.addInputCondition(new FieldCondition("field2", "value"))
					)
					.addInputCondition(new OrBlockOperation()
						.addInputCondition(new FieldCondition("field3", "value"))
						.addInputCondition(new FieldCondition("field4", "value"))
					)
			);		
		assertElement(expectedElement, elements.get(0));
	}

    @Test
    public void testPathSpacePresent() throws RestException {
        PathParser parser = new PathParser("Entities(field='Den Haag')");
        List<InputElement> elements = getElements(parser);
        Assert.assertEquals("There should be one input elements", 1, elements.size());

        InputElement expectedElement = new InputElement("Entities").setCondition(new FieldCondition("field", "Den Haag"));
        assertElement(expectedElement, elements.get(0));
    }

	@Test
	public void testPathColonPresent() throws RestException {
		PathParser parser = new PathParser("Entities(deviceId='3:0')");
		List<InputElement> elements = getElements(parser);
		Assert.assertEquals("There should be one input elements", 1, elements.size());

		InputElement expectedElement = new InputElement("Entities").setCondition(new FieldCondition("deviceId", "3:0"));
		assertElement(expectedElement, elements.get(0));
	}

	@Test
	public void testPathEqualsPresent() throws RestException {
        PathParser parser = new PathParser("Entities(token='smg1adfefi0cylwt7kzfc8wcyhz0bxt-noxacuozrn4=')");
        List<InputElement> elements = getElements(parser);
        assertThat(elements.size(), is(1));

        InputElement expectedElement = new InputElement("Entities").setCondition(new FieldCondition("token", "smg1adfefi0cylwt7kzfc8wcyhz0bxt-noxacuozrn4="));
        assertElement(expectedElement, elements.get(0));
	}

	@Test
	public void testComplexPath() throws RestException {
		String testPath = "test/element(test=test,check<20,age>10,field>=10,field2<=20)/instance(name=test)";
		PathParser parser = new PathParser(testPath);
		List<InputElement> elements = getElements(parser);
		Assert.assertEquals("There should be three input elements", 3, elements.size());
		
		InputElement firstElement = elements.get(0);
		InputElement secondElement = elements.get(1);
		InputElement lastElement = elements.get(2);
		
		InputElement firstExpectedElement = new InputElement("test");
		assertElement(firstExpectedElement, firstElement);
		
		InputElement secondExpectedElement = new InputElement("element").setCondition(
			new AndBlockOperation()
			.addInputCondition(new FieldCondition("test", "test"))
			.addInputCondition(new FieldCondition("check", Operator.Smaller_than, "20"))
			.addInputCondition(new FieldCondition("age", Operator.Larger_than, "10"))
			.addInputCondition(new FieldCondition("field", Operator.Larger_than_or_Equals, "10"))
			.addInputCondition(new FieldCondition("field2", Operator.Smaller_than_or_Equals, "20"))
		);		
		assertElement(secondExpectedElement, secondElement);
		
		InputElement lastExpectedElement = new InputElement("instance").setCondition(new FieldCondition("name", "test"));
		assertElement(lastExpectedElement, lastElement);		
	}
	
	@Test(expected=SyntaxException.class)
	public void testInvalidPathParameters() throws RestException {
		String testPath = "element(test==)";
		new PathParser(testPath);
	}
	
	@Test(expected=SyntaxException.class)
	public void testEmptyPath() throws RestException {
		new PathParser("");
	}
	
	@Test(expected=SyntaxException.class)
	public void testInvalidPathBrackets() throws SyntaxException {
		new PathParser("Bag(test");
	}
	
	@Test(expected=SyntaxException.class)
	public void testInvalidPathSeperators() throws SyntaxException {
		new PathParser("Bag(test)ttt");
	}

    @Test(expected=SyntaxException.class)
    public void testInvalidQueryBlocks() throws SyntaxException {
        new PathParser("Entities((field1=value1)(field1=value50))");
    }

    @Test
    public void testBlockStart() throws SyntaxException {
        PathParser parser = new PathParser("Entities((field1=value1)|(field1=value50))");
        List<InputElement> elements = getElements(parser);

        Assert.assertEquals("There should be one input elements", 1, elements.size());

        InputElement expectedElement = new InputElement("Entities").setCondition(
                new OrBlockOperation()
                        .addInputCondition(new FieldCondition("field1", "value1"))
                        .addInputCondition(new FieldCondition("field1", "value50"))
        );
        assertElement(expectedElement, elements.get(0));

    }

	@Test
	public void testPathById() throws RestException {
		PathParser parser = new PathParser("Bag(test)");
		List<InputElement> elements = getElements(parser);
		Assert.assertEquals("There should be one input elements", 1, elements.size());
		
		InputElement expectedElement = new InputElement("Bag").setCondition(new FieldCondition("ID", "test"));
		assertElement(expectedElement, elements.get(0));
	}

    @Test(expected = RestException.class)
    public void testPathInvalidEmptyValue() throws RestException {
        new PathParser("Instance(default)/Bags(inverted)/Entities(field1=)");
    }

    @Test(expected = RestException.class)
    public void testPathInvalidEmptyValueLargerThan() throws RestException {
        new PathParser("Instance(default)/Bags(inverted)/Entities(field1>=)");
    }

    @Test
    public void testPathEscapedStringValue() throws RestException {
        PathParser parser = new PathParser("Instance(default)/Bags(inverted)/Entities(field1='value')");
        List<InputElement> elements = getElements(parser);
        assertEquals("There should be three elements", 3, elements.size());

        InputElement expectedElement = new InputElement("Entities").setCondition(new FieldCondition("field1", "value"));
        assertElement(expectedElement, elements.get(2));
    }

    @Test
    public void testPathNestedEntity() throws RestException {
        PathParser parser = new PathParser("Instance(default)/Bags(inverted)/Entities(embed.field1='value')");
        List<InputElement> elements = getElements(parser);
        assertEquals("There should be three elements", 3, elements.size());

        InputElement expectedElement = new InputElement("Entities").setCondition(new FieldCondition("embed.field1", "value"));
        assertElement(expectedElement, elements.get(2));

    }

    @Test
    public void testPathEscapedMoreQuotesStringValue() throws RestException {
        PathParser parser = new PathParser("Instance(default)/Bags(inverted)/Entities(field1='va'lue')");
        List<InputElement> elements = getElements(parser);
        assertEquals("There should be three elements", 3, elements.size());

        InputElement expectedElement = new InputElement("Entities").setCondition(new FieldCondition("field1", "va'lue"));
        assertElement(expectedElement, elements.get(2));
    }

    @Test(expected = SyntaxException.class)
    public void testPathStringQuotesInvalid() throws RestException {
        new PathParser("Instance(default)/Bags(inverted)/Entities(field1='va'lue)");
    }

    @Test
    public void testPathEmptyStringValue() throws RestException {
        PathParser parser = new PathParser("Instance(default)/Bags(inverted)/Entities(field1='')");
        List<InputElement> elements = getElements(parser);
        assertEquals("There should be three elements", 3, elements.size());

        InputElement expectedElement = new InputElement("Entities").setCondition(new FieldCondition("field1", ""));
        assertElement(expectedElement, elements.get(2));
    }

    @Test
    public void testPathNonStandardCharacters() throws RestException {
        PathParser parser = new PathParser("Instance(default)/Bags(inverted)/Entities(field1=coëfficiënt van poisson)");
        List<InputElement> elements = getElements(parser);
        assertEquals("There should be three elements", 3, elements.size());

        InputElement expectedElement = new InputElement("Entities").setCondition(new FieldCondition("field1", "coëfficiënt van poisson"));
        assertElement(expectedElement, elements.get(2));
    }
    
    @Test
    public void testPathUUID() throws RestException {
        String randomId = UUID.randomUUID().toString();
        PathParser parser = new PathParser("Partitions(" + randomId + ")");
        List<InputElement> elements = getElements(parser);
        Assert.assertEquals("There should be one input elements", 1, elements.size());

        InputElement expectedElement = new InputElement("Partitions").setCondition(new FieldCondition("ID", randomId));
        assertElement(expectedElement, elements.get(0));
    }
	
	private void assertElement(InputElement expected, InputElement actual) {
		Assert.assertEquals("Unexpected element name", expected.getElementName(), actual.getElementName());
		assertInputCondition(expected.getCondition(), actual.getCondition());
	}
	
	private void assertInputCondition(InputCondition expected, InputCondition actual) {
		if(expected != null) {
			Assert.assertNotNull("No conditions found", actual);
			Assert.assertEquals("Unexpected condition type", expected.getClass(), actual.getClass());
			
			if(expected instanceof FieldCondition) {
				FieldCondition expectedFieldCondition = (FieldCondition) expected;
				FieldCondition actualFieldCondition = (FieldCondition) actual;
				
				Assert.assertEquals("Unexpected field name", expectedFieldCondition.getField(), actualFieldCondition.getField());
				Assert.assertEquals("Unexpected field operator", expectedFieldCondition.getOperator(), actualFieldCondition.getOperator());
				Assert.assertEquals("Unexpected field value", expectedFieldCondition.getValue(), actualFieldCondition.getValue());
			} else if(expected instanceof BlockOperation) {
				BlockOperation expectedBlockOperation = (BlockOperation) expected;
				BlockOperation actualBlockOperation = (BlockOperation) actual;
				
				Assert.assertEquals("Unexpected number of child conditions", 
						expectedBlockOperation.getChildConditions().size(), actualBlockOperation.getChildConditions().size());
				
				for(int i=0; i<expectedBlockOperation.getChildConditions().size(); i++) {
					InputCondition expectedChildCondition = expectedBlockOperation.getChildConditions().get(i);
					InputCondition actualChildCondition = actualBlockOperation.getChildConditions().get(i);
					
					assertInputCondition(expectedChildCondition, actualChildCondition);
				}
			}
		}
	}
	
	private List<InputElement> getElements(PathParser parser) {
		List<InputElement> elements = new ArrayList<>();
		for(InputElement element : parser) {
			elements.add(element);
		}
		
		return elements;
	}
}
