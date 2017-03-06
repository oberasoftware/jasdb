/*
 * The JASDB software and code is Copyright protected 2012 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2012 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package com.oberasoftware.jasdb.engine.query;

import com.oberasoftware.jasdb.engine.query.operators.AndBlock;
import com.oberasoftware.jasdb.engine.query.operators.BlockOperation;
import com.oberasoftware.jasdb.engine.query.operators.OrBlock;
import nl.renarj.jasdb.api.query.QueryBuilder;
import nl.renarj.jasdb.api.query.QueryField;
import nl.renarj.jasdb.index.keys.Key;
import nl.renarj.jasdb.index.search.EqualsCondition;
import nl.renarj.jasdb.index.search.NotEqualsCondition;
import nl.renarj.jasdb.index.search.RangeCondition;
import nl.renarj.jasdb.index.search.SearchCondition;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * User: renarj
 * Date: 4/28/12
 * Time: 2:21 PM
 */
public class BuilderTransformer {
    public static BlockOperation transformBuilder(QueryBuilder builder) {
        BlockOperation block = null;
        switch(builder.getBlockType()) {
            case AND:
                block = new AndBlock();
                break;
            case OR:
                block = new OrBlock();
                break;
        }

        for(Map.Entry<String, List<QueryField>> entry : builder.getQueryFields().entrySet()) {
            addQueryFieldToBlock(block, entry.getKey(), entry.getValue());
        }

        for(QueryBuilder childBuilder : builder.getChildBuilders()) {
            block.addChildBlock(transformBuilder(childBuilder));
        }

        return block;
    }

    private static void addQueryFieldToBlock(BlockOperation block, String field, List<QueryField> fields) {
        List<RangeCondition> rangeConditions = new ArrayList<>();
        for(QueryField queryField : fields) {
            SearchCondition searchCondition = createCondition(queryField);
            if(searchCondition instanceof RangeCondition) {
                rangeConditions.add((RangeCondition)searchCondition);
            } else {
                block.addCondition(field, searchCondition);
            }
        }

        if(rangeConditions.size() > 1) {
            block.addCondition(field, mergeRangeConditions(rangeConditions));
        } else if(!rangeConditions.isEmpty()){
            block.addCondition(field, rangeConditions.get(0));
        }
    }

    private static RangeCondition mergeRangeConditions(List<RangeCondition> rangeConditions) {
        Key lowestKey = null;
        boolean startInclusive = false;
        boolean endInclusive = false;
        Key highestKey = null;

        for(SearchCondition condition : rangeConditions) {
            RangeCondition rangeCondition = (RangeCondition) condition;
            Key rangeStartKey = rangeCondition.getStart();
            Key rangeEndKey = rangeCondition.getEnd();

            if(rangeStartKey != null && (lowestKey == null || rangeStartKey.compareTo(lowestKey) < 0)) {
                lowestKey = rangeStartKey;
                startInclusive = rangeCondition.isStartIncluded();
            }

            if(rangeEndKey != null && (highestKey == null || rangeEndKey.compareTo(highestKey) > 0)) {
                highestKey = rangeEndKey;
                endInclusive = rangeCondition.isEndIncluded();
            }
        }

        return new RangeCondition(lowestKey, startInclusive, highestKey, endInclusive);
    }

    private static SearchCondition createCondition(QueryField queryField) {
        switch(queryField.getOperator()) {
            case LARGER_THAN:
                return new RangeCondition(queryField.getSearchKey(), false, null, false);
            case LARGER_THAN_OR_EQUALS:
                return new RangeCondition(queryField.getSearchKey(), true, null, false);
            case SMALLER_THAN:
                return new RangeCondition(null,  false, queryField.getSearchKey(), false);
            case SMALLER_THAN_OR_EQUALS:
                return new RangeCondition(null,  false, queryField.getSearchKey(), true);
            case NOT_EQUALS:
                return new NotEqualsCondition(queryField.getSearchKey());
            case EQUALS:
            default:
                return new EqualsCondition(queryField.getSearchKey());
        }
    }

}
