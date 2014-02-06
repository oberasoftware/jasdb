package nl.renarj.jasdb.rest.providers;

import nl.renarj.jasdb.rest.exceptions.RestException;
import nl.renarj.jasdb.rest.model.ErrorEntity;
import nl.renarj.jasdb.rest.model.RestEntity;
import nl.renarj.jasdb.rest.serializers.RestResponseHandler;
import nl.renarj.jasdb.rest.serializers.json.JsonRestResponseHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.OutputStream;

/**
 * @author Renze de Vries
 */
public class ServiceOutputHandler {
    private static final Logger log = LoggerFactory.getLogger(ServiceOutputHandler.class);

    private static final JsonRestResponseHandler restResponseHandler = new JsonRestResponseHandler();

    public static Response createResponse(final RestEntity entity) {
        if(entity != null) {
            if(entity instanceof ErrorEntity) {
                return handleError((ErrorEntity)entity);
            } else {
                return Response.ok(createStreamOutput(entity), restResponseHandler.getMediaType()).build();
            }
        } else {
            return handleError(new ErrorEntity(Response.Status.NOT_FOUND.getStatusCode(), "Resource could not be found"));
        }
    }

    public static Response handleError(String message) {
        return handleError(new ErrorEntity(Response.Status.BAD_REQUEST.getStatusCode(), message));
    }

    private static Response handleError(ErrorEntity errorEntity)  {
        return Response.status(errorEntity.getStatusCode()).entity(createStreamOutput(errorEntity)).build();
    }

    public static RestResponseHandler getResponseHandler() {
        return restResponseHandler;
    }

    private static StreamingOutput createStreamOutput(final RestEntity entity) {
        return new StreamingOutput() {
            @Override
            public void write(OutputStream outputStream) throws IOException, WebApplicationException {
                try {
                    restResponseHandler.serialize(entity, outputStream);
                } catch(RestException e) {
                    log.debug("Stream error, full stack", e);
                    log.info("Could not stream the entity: " + e.getMessage());
                } catch(Throwable e) {
                    log.error("Unable to stream out entity", e);
                }
            }
        };
    }
}
