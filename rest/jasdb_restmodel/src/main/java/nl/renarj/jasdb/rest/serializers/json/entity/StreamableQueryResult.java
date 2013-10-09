package nl.renarj.jasdb.rest.serializers.json.entity;

import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.api.query.QueryResult;
import nl.renarj.jasdb.api.serializer.json.JsonEntityDeserializer;
import nl.renarj.jasdb.core.exceptions.MetadataParseException;
import nl.renarj.jasdb.core.exceptions.RuntimeJasDBException;
import org.codehaus.jackson.JsonParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

/**
* @author Renze de Vries
*/
public class StreamableQueryResult implements QueryResult {
    private static final Logger log = LoggerFactory.getLogger(StreamableQueryResult.class);

    private JsonEntityDeserializer entityDeserializer = new JsonEntityDeserializer();
    private JsonParser parser;
    private long size;
    private SimpleEntity currentEntity = null;
    private InputStream inputStream;
    private boolean closed = false;

    private long lastUsage = System.currentTimeMillis();

    public StreamableQueryResult(JsonParser parser, long size, InputStream inputStream) {
        this.parser = parser;
        this.size = size;
        this.inputStream = inputStream;
        StreamingQueryMonitor.registerQueryCurors(this);
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public Iterator<SimpleEntity> iterator() {
        return this;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    public long getLastUsage() {
        return lastUsage;
    }

    @Override
    public boolean hasNext() {
        checkClosed();
        if(currentEntity == null) {
            try {
                currentEntity = entityDeserializer.deserializeEntity(parser);
                if(currentEntity != null) {
                    lastUsage = System.currentTimeMillis();
                    return true;
                } else {
                    close();
                }
            } catch(MetadataParseException e) {
                log.error("Unable to parse entity", e);
            }
        }

        return currentEntity != null;
    }

    @Override
    public void close() {
        if(!closed) {
            log.debug("Closing streaming entity collection resources");
            closed = true;
            try {
                parser.close();
                inputStream.close();
            } catch(IOException e) {
                log.error("Unable to cleanly close the query result stream", e);
            }
        }
    }

    private void checkClosed() {
        if(closed) {
            throw new RuntimeJasDBException("Query cursor is closed");
        }
    }

    @Override
    public SimpleEntity next() {
        checkClosed();
        if(currentEntity != null || hasNext()) {
            SimpleEntity returnEntity = currentEntity;
            currentEntity = null;
            return returnEntity;
        } else {
            return null;
        }
    }

    @Override
    public void remove() {
        throw new RuntimeException("Not implemented");
    }
}
