package nl.renarj.jasdb.rest.input;

import nl.renarj.core.utilities.StringUtils;
import nl.renarj.jasdb.rest.exceptions.SyntaxException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Renze de Vries
 *         Date: 15-6-12
 *         Time: 13:11
 */
public class OrderParameterParsing {
    private static final Pattern ORDERBY_PATTERN = Pattern.compile("((\\w+) (asc|ASC)|(\\w+) (desc|DESC)|(\\w+))");

    public static List<OrderParam> getOrderParams(String orderBy) throws SyntaxException {
        if(StringUtils.stringNotEmpty(orderBy)) {
            Matcher orderMatcher = ORDERBY_PATTERN.matcher(orderBy);
            List<OrderParam> orderParams = new ArrayList<>();
            while(orderMatcher.find()) {
                String ascendingField = orderMatcher.group(2);
                String descendingField = orderMatcher.group(4);
                String noSortDirectionField = orderMatcher.group(6);
                if(StringUtils.stringNotEmpty(ascendingField)) {
                    orderParams.add(new OrderParam(ascendingField, OrderParam.DIRECTION.ASC));
                } else if(StringUtils.stringNotEmpty(descendingField)) {
                    orderParams.add(new OrderParam(descendingField, OrderParam.DIRECTION.DESC));
                } else if(StringUtils.stringNotEmpty(noSortDirectionField)) {
                    orderParams.add(new OrderParam(noSortDirectionField, OrderParam.DIRECTION.ASC));
                } else {
                    throw new SyntaxException("Unexpected order by syntax: " + orderBy);
                }
            }
            return orderParams;
        } else {
            return Collections.emptyList();
        }
    }
}
