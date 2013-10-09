package nl.renarj.jasdb.rest.input;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author Renze de Vries
 */
public class InputScannerTest {

    @Test
    public void testNonStandardCharacters() {
        InputScanner scanner = new InputScanner("field1=coëfficiënt van poisson");
        assertTokens(scanner, "field1", "=", "coëfficiënt van poisson");
    }

    @Test
    public void testStringEscapeChars() {
        InputScanner scanner = new InputScanner("field1='value'");
        assertTokens(scanner, "field1", "=", "'value'");

        scanner = new InputScanner("field1>='value'");
        assertTokens(scanner, "field1", ">", "=", "'value'");

        scanner = new InputScanner("field1<='value'");
        assertTokens(scanner, "field1", "<", "=", "'value'");
    }

    @Test
    public void testUnderscoreName() {
        InputScanner scanner = new InputScanner("name_some_underscores");
        assertTokens(scanner, "name_some_underscores");
    }

    @Test
    public void testDoubleStringEscape() {
        InputScanner scanner = new InputScanner("field1='va'lue'");
        assertTokens(scanner, "field1", "=", "'va'lue'");
    }

    @Test
    public void testScannerFieldValue() {
        InputScanner scanner = new InputScanner("field1=value");
        assertTokens(scanner, "field1", "=", "value");
    }

    @Test
    public void testFieldLargerThanEquals() {
        InputScanner scanner = new InputScanner("field1>=value");
        assertTokens(scanner, "field1", ">", "=", "value");
    }

    @Test
    public void testFieldLarger() {
        InputScanner scanner = new InputScanner("field1>value");
        assertTokens(scanner, "field1", ">", "value");
    }

    @Test
    public void testFieldSmaller() {
        InputScanner scanner = new InputScanner("field1<value");
        assertTokens(scanner, "field1", "<", "value");
    }

    @Test
    public void testFieldSmallerThanEquals() {
        InputScanner scanner = new InputScanner("field1<=value");
        assertTokens(scanner, "field1", "<", "=", "value");
    }

    @Test
    public void testFieldOrField() {
        InputScanner scanner = new InputScanner("field1=value|field2=value");
        assertTokens(scanner, "field1", "=", "value", "|", "field2", "=", "value");
    }

    @Test
    public void testFieldAndField() {
        InputScanner scanner = new InputScanner("field1=value,field2=value");
        assertTokens(scanner, "field1", "=", "value", ",", "field2", "=", "value");
    }

    @Test
    public void testFieldComplex() {
        InputScanner scanner = new InputScanner("field1=value|(field2=value,field3=anotherValue With Spaces)");
        assertTokens(scanner, "field1", "=", "value", "|", "(", "field2", "=", "value", ",", "field3", "=", "anotherValue With Spaces", ")");
    }

    @Test
    public void testNotEquals() {
        InputScanner scanner = new InputScanner("field1!=value");
        assertTokens(scanner, "field1", "!", "=", "value");
    }

    private void assertTokens(InputScanner scanner, String... expectedTokens) {
        for(String expectedToken : expectedTokens) {
            String actual = scanner.nextToken();
            assertNotNull(actual);
            assertEquals(expectedToken, actual);
        }
    }
}
