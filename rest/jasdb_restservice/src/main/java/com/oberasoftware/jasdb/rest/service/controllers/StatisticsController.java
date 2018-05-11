package com.oberasoftware.jasdb.rest.service.controllers;

import com.oberasoftware.jasdb.api.exceptions.RestException;
import com.oberasoftware.jasdb.core.statistics.AggregationResult;
import com.oberasoftware.jasdb.core.statistics.StatisticsMonitor;
import com.oberasoftware.jasdb.rest.model.RestEntity;
import com.oberasoftware.jasdb.rest.model.Statistic;
import com.oberasoftware.jasdb.rest.model.StatisticCollection;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedList;
import java.util.List;

/**
 * @author Renze de Vries
 * Date: 1-5-12
 * Time: 21:40
 */
@RestController
public class StatisticsController {
    @RequestMapping(value = "/stats", method = RequestMethod.GET, produces = "application/json")
    public RestEntity loadModel() throws RestException {
        List<AggregationResult> stats = StatisticsMonitor.getAggregationResults();
        List<Statistic> mappedStats = new LinkedList<>();
        for(AggregationResult stat : stats) {
            mappedStats.add(new Statistic(stat.getName(), stat.getAverage(), stat.getCalls(), stat.getTotalTime(), stat.getLowest(), stat.getHighest()));
        }

        return new StatisticCollection(mappedStats);
    }

    @RequestMapping(value = "/stats/reset")
    public RestEntity doOperation() throws RestException {
        StatisticsMonitor.clearStats();

        return new StatisticCollection(new LinkedList<>());
    }
}
