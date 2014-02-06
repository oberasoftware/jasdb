package nl.renarj.jasdb.rest.client;

import nl.renarj.jasdb.api.query.Order;
import nl.renarj.jasdb.api.query.SortParameter;
import nl.renarj.jasdb.index.keys.impl.LongKey;
import nl.renarj.jasdb.index.keys.impl.StringKey;
import nl.renarj.jasdb.index.search.EqualsCondition;
import nl.renarj.jasdb.index.search.RangeCondition;
import nl.renarj.jasdb.remote.exceptions.RemoteException;
import nl.renarj.jasdb.storage.query.operators.AndBlock;
import nl.renarj.jasdb.storage.query.operators.OrBlock;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author: renarj
 * Date: 13-4-12
 * Time: 21:12
 */
public class RestQueryGeneratorTest {
    @Test
    public void testComplexQueryGenerator() throws RemoteException {
        OrBlock orBlock = new OrBlock();

        AndBlock block1 = new AndBlock();
        block1.addCondition("field1", new EqualsCondition(new StringKey("value1")));
        block1.addCondition("field2", new EqualsCondition(new LongKey(1099)));

        AndBlock block2 = new AndBlock();
        block2.addCondition("field3", new RangeCondition(new LongKey(10), true, new LongKey(100), true));
        orBlock.addChildBlock(block1);
        orBlock.addChildBlock(block2);

        orBlock.addCondition("age", new EqualsCondition(new StringKey("100")));

        String generatedQuery = RestQueryGenerator.generatorQuery(orBlock);
        assertTrue("Unexpected Query", generatedQuery.contains("age='100'"));
        assertTrue("Unexpected Query", generatedQuery.contains("field3>=10"));
        assertTrue("Unexpected Query", generatedQuery.contains("field3<=100"));
        assertTrue("Unexpected Query", generatedQuery.contains("field2=1099"));
        assertTrue("Unexpected Query", generatedQuery.contains("field1='value1'"));

        assertEquals("Unexpected query", 2, gatherCharacterCount('|', generatedQuery));
        assertEquals("Unexpected query", 2, gatherCharacterCount(',', generatedQuery));
        assertEquals("Unexpected query", 2, gatherCharacterCount('(', generatedQuery));
        assertEquals("Unexpected query", 2, gatherCharacterCount(')', generatedQuery));
    }

    private int gatherCharacterCount(char c, String s) {
        int counter = 0;
        for(char k : s.toCharArray()) {
            if(k == c) {
                counter++;
            }
        }
        return counter;
    }

    @Test
    public void testGenerateOrderParams() {
        List<SortParameter> sortParameterList = new ArrayList<>();
        sortParameterList.add(new SortParameter("field1", Order.ASCENDING));

        assertEquals("Unexpected order params", "field1 ASC", RestQueryGenerator.generateOrderParams(sortParameterList));

        sortParameterList.add(new SortParameter("field3", Order.DESCENDING));
        assertEquals("Unexpected order params", "field1 ASC,field3 DESC", RestQueryGenerator.generateOrderParams(sortParameterList));
    }
}
