package nl.renarj.jasdb.rest.input;

import nl.renarj.jasdb.rest.exceptions.SyntaxException;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * @author Renze de Vries
 *         Date: 15-6-12
 *         Time: 13:28
 */
public class OrderParameterParsingTest {
    @Test
    public void testOrderBySingleParam() throws SyntaxException {
        List<OrderParam> orderParamList = OrderParameterParsing.getOrderParams("field1 asc");
        assertEquals("There should be one order param", 1, orderParamList.size());
        assertEquals("Field should be field1", "field1", orderParamList.get(0).getField());
        assertEquals("Order direction should be asc", OrderParam.DIRECTION.ASC, orderParamList.get(0).getSortDirection());

        orderParamList = OrderParameterParsing.getOrderParams("field1 desc");
        assertEquals("There should be one order param", 1, orderParamList.size());
        assertEquals("Field should be field1", "field1", orderParamList.get(0).getField());
        assertEquals("Order direction should be asc", OrderParam.DIRECTION.DESC, orderParamList.get(0).getSortDirection());
    }

    @Test
    public void testOrderByMultiParam() throws SyntaxException {
        List<OrderParam> orderParamList = OrderParameterParsing.getOrderParams("field1 asc,field2 desc");
        assertEquals("There should be two order params", 2, orderParamList.size());
        assertEquals("Field should be field1", "field1", orderParamList.get(0).getField());
        assertEquals("Order direction should be asc", OrderParam.DIRECTION.ASC, orderParamList.get(0).getSortDirection());
        assertEquals("Field should be field1", "field2", orderParamList.get(1).getField());
        assertEquals("Order direction should be desc", OrderParam.DIRECTION.DESC, orderParamList.get(1).getSortDirection());
    }

    @Test
    public void testOrderByNoSortDirection() throws SyntaxException {
        List<OrderParam> orderParamList = OrderParameterParsing.getOrderParams("field1 asc,field2 desc, field3");
        assertEquals("There should be two order params", 3, orderParamList.size());
        assertEquals("Field should be field1", "field1", orderParamList.get(0).getField());
        assertEquals("Order direction should be asc", OrderParam.DIRECTION.ASC, orderParamList.get(0).getSortDirection());
        assertEquals("Field should be field1", "field2", orderParamList.get(1).getField());
        assertEquals("Order direction should be desc", OrderParam.DIRECTION.DESC, orderParamList.get(1).getSortDirection());
        assertEquals("Field should be field1", "field3", orderParamList.get(2).getField());
        assertEquals("Order direction should be asc", OrderParam.DIRECTION.ASC, orderParamList.get(2).getSortDirection());
    }

    @Test
    public void testOrderByCapitalCased() throws SyntaxException {
        List<OrderParam> orderParamList = OrderParameterParsing.getOrderParams("field1 ASC");
        assertEquals("There should be one order param", 1, orderParamList.size());
        assertEquals("Field should be field1", "field1", orderParamList.get(0).getField());
        assertEquals("Order direction should be asc", OrderParam.DIRECTION.ASC, orderParamList.get(0).getSortDirection());

        orderParamList = OrderParameterParsing.getOrderParams("field1 DESC");
        assertEquals("There should be one order param", 1, orderParamList.size());
        assertEquals("Field should be field1", "field1", orderParamList.get(0).getField());
        assertEquals("Order direction should be asc", OrderParam.DIRECTION.DESC, orderParamList.get(0).getSortDirection());
    }

    @Test
    public void testEmptySort() throws SyntaxException {
        List<OrderParam> params = OrderParameterParsing.getOrderParams("");
        assertEquals("There should be no sort params", 0, params.size());

        params = OrderParameterParsing.getOrderParams(null);
        assertEquals("There should be no sort params", 0, params.size());
    }
}
