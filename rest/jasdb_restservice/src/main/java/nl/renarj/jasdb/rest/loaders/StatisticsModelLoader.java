package nl.renarj.jasdb.rest.loaders;

import nl.renarj.core.statistics.AggregationResult;
import nl.renarj.core.statistics.StatisticsMonitor;
import nl.renarj.jasdb.api.context.RequestContext;
import nl.renarj.jasdb.rest.exceptions.RestException;
import nl.renarj.jasdb.rest.input.InputElement;
import nl.renarj.jasdb.rest.input.OrderParam;
import nl.renarj.jasdb.rest.model.RestEntity;
import nl.renarj.jasdb.rest.model.Statistic;
import nl.renarj.jasdb.rest.model.StatisticCollection;
import nl.renarj.jasdb.rest.serializers.RestResponseHandler;
import org.springframework.stereotype.Component;

import java.util.LinkedList;
import java.util.List;

/**
 * @author: renarj
 * Date: 1-5-12
 * Time: 21:40
 */
@Component
public class StatisticsModelLoader extends AbstractModelLoader {
    @Override
    public String[] getModelNames() {
        return new String[] {"Statistics", "stats"};
    }

    @Override
    public RestEntity loadModel(InputElement input, String begin, String top, List<OrderParam> orderParamList, RequestContext context) throws RestException {
        List<AggregationResult> stats = StatisticsMonitor.getAggregationResults();
        List<Statistic> mappedStats = new LinkedList<>();
        for(AggregationResult stat : stats) {
            mappedStats.add(new Statistic(stat.getName(), stat.getAverage(), stat.getCalls(), stat.getTotalTime(), stat.getLowest(), stat.getHighest()));
        }

        return new StatisticCollection(mappedStats);
    }

    @Override
    public RestEntity doOperation(InputElement input) throws RestException {
        String operation = input.getElementName();
        if("reset".equals(operation)) {
            StatisticsMonitor.clearStats();
        }

        return new StatisticCollection(new LinkedList<Statistic>());
    }

    @Override
    public boolean isOperationSupported(String operation) {
        return "reset".equals(operation);
    }

    @Override
    public RestEntity writeEntry(InputElement input, RestResponseHandler serializer, String rawData, RequestContext context) throws RestException {
        throw new RestException("Unsupported operation, cannot write to statistics");
    }
}
