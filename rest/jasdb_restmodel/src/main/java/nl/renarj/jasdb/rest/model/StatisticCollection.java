package nl.renarj.jasdb.rest.model;

import java.util.List;

/**
 * @author: renarj
 * Date: 1-5-12
 * Time: 21:45
 */
public class StatisticCollection implements RestEntity {
    private List<Statistic> stats;
    public StatisticCollection(List<Statistic> stats) {
        this.stats = stats;
    }

    public List<Statistic> getStats() {
        return stats;
    }

    public void setStats(List<Statistic> stats) {
        this.stats = stats;
    }
}
