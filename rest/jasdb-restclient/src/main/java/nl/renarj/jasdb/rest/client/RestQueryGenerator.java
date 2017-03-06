package nl.renarj.jasdb.rest.client;

import com.oberasoftware.jasdb.engine.query.operators.AndBlock;
import com.oberasoftware.jasdb.engine.query.operators.BlockOperation;
import com.oberasoftware.jasdb.engine.query.operators.OrBlock;
import nl.renarj.jasdb.api.query.Order;
import nl.renarj.jasdb.api.query.SortParameter;
import nl.renarj.jasdb.index.keys.Key;
import nl.renarj.jasdb.index.keys.impl.LongKey;
import nl.renarj.jasdb.index.search.EqualsCondition;
import nl.renarj.jasdb.index.search.NotEqualsCondition;
import nl.renarj.jasdb.index.search.RangeCondition;
import nl.renarj.jasdb.index.search.SearchCondition;
import nl.renarj.jasdb.remote.exceptions.RemoteException;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author Renze de Vries
 * Date: 13-4-12
 * Time: 11:23
 */
public class RestQueryGenerator  {
    private static final String ASC_KEYWORD = "ASC";
    private static final String DESC_KEYWORD = "DESC";
    private static final String ORDER_SPLIT = ",";
    public static final String STRING_ESCAPE = "'";

    private RestQueryGenerator() {

    }

    public static String generatorQuery(BlockOperation block) throws RemoteException {
        StringBuilder builder = new StringBuilder();
        handleBlockOperation(builder, block);
        return builder.toString();
    }

    private static void generateCondition(StringBuilder builder, String field, SearchCondition condition) throws RemoteException {
        if(condition instanceof NotEqualsCondition) {
            generateNotEqualsCondition(builder, field, (NotEqualsCondition) condition);
        } else if(condition instanceof EqualsCondition) {
            generateEqualsCondition(builder, field, (EqualsCondition) condition);
        } else if(condition instanceof RangeCondition) {
            generateRangeCondition(builder, field, (RangeCondition) condition);
        } else {
            throw new RemoteException("Unknown condition type: " + condition.getClass().toString());
        }
    }

    private static void generateNotEqualsCondition(StringBuilder builder, String field, NotEqualsCondition notEqualsCondition) {
        Key key = notEqualsCondition.getKey();
        builder.append(field).append("!=");
        handleValueAppend(builder, key);
    }

    private static void generateEqualsCondition(StringBuilder builder, String field, EqualsCondition equalsCondition) {
        Key key = equalsCondition.getKey();
        builder.append(field).append("=");
        handleValueAppend(builder, key);
    }

    private static void generateRangeCondition(StringBuilder builder, String field, RangeCondition rangeCondition) {
        Key startKey = rangeCondition.getStart();
        Key endKey = rangeCondition.getEnd();

        if(startKey != null) {
            builder.append(field).append(rangeCondition.isStartIncluded() ? ">=" : ">");
            handleValueAppend(builder, startKey);
        }
        if(startKey != null && endKey != null) {
            builder.append(",");
        }
        if(endKey != null) {
            builder.append(field).append(rangeCondition.isEndIncluded() ? "<=" : "<");
            handleValueAppend(builder, endKey);
        }
    }

    private static void handleBlockOperation(StringBuilder builder, BlockOperation blockOperation) throws RemoteException {
        Map<String, Set<SearchCondition>> blockFieldConditions = blockOperation.getConditions();

        String blockOperator = "";
        if(blockOperation instanceof AndBlock) blockOperator = ",";
        if(blockOperation instanceof OrBlock) blockOperator = "|";

        boolean first = true;
        for(Map.Entry<String, Set<SearchCondition>> blockConditions : blockFieldConditions.entrySet()) {
            handleBlockFieldOperation(builder, blockConditions.getKey(), blockConditions.getValue(), blockOperator, first);
            first = false;
        }

        for(BlockOperation childBlock : blockOperation.getChildBlocks()) {
            if(!first) builder.append(blockOperator);

            builder.append("(");
            handleBlockOperation(builder, childBlock);
            builder.append(")");
            first = false;
        }
    }

    private static void handleBlockFieldOperation(StringBuilder builder, String field, Set<SearchCondition> conditions, String blockOperator, boolean first) throws RemoteException {
        for(SearchCondition condition : conditions) {
            if(!first) {
                builder.append(blockOperator);
            }
            first = false;
            generateCondition(builder, field, condition);
        }
    }

    private static void handleValueAppend(StringBuilder builder, Key key) {
        if(key instanceof LongKey) {
            builder.append(key.getValue());
        } else {
            builder.append(STRING_ESCAPE).append(key.getValue()).append(STRING_ESCAPE);
        }
    }

    public static String generateOrderParams(List<SortParameter> sortParameterList) {
        StringBuilder orderBuilder = new StringBuilder();
        boolean first = true;
        for(SortParameter sortParameter : sortParameterList) {
            if(!first) orderBuilder.append(ORDER_SPLIT); else first = false;

            String orderingKeyword = sortParameter.getOrder() == Order.ASCENDING ? ASC_KEYWORD : DESC_KEYWORD;
            orderBuilder.append(sortParameter.getField()).append(" ").append(orderingKeyword);
        }
        return orderBuilder.toString();
    }
}
