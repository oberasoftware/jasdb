package nl.renarj.jasdb.rest.model.streaming;

import nl.renarj.jasdb.api.query.QueryResult;
import nl.renarj.jasdb.rest.model.RestEntity;

/**
 * @author Renze de Vries
 */
public class StreamableEntityCollection implements RestEntity {
    private QueryResult result;
    private long size;
    private long timeMilliseconds;

    public StreamableEntityCollection(QueryResult result) {
        this.result = result;
        this.size = result.size();
    }

    public void setTimeMilliseconds(long timeMilliseconds) {
        this.timeMilliseconds = timeMilliseconds;
    }

    public QueryResult getResult() {
        return result;
    }

    public long getSize() {
        return size;
    }

    public long getTimeMilliseconds() {
        return timeMilliseconds;
    }

}
