package com.oberasoftware.jasdb.rest.service.controllers;

import com.oberasoftware.jasdb.api.exceptions.RestException;
import com.oberasoftware.jasdb.rest.model.ErrorEntity;
import com.oberasoftware.jasdb.rest.model.RestEntity;
import com.oberasoftware.jasdb.rest.model.serializers.RestResponseHandler;
import com.oberasoftware.jasdb.rest.model.serializers.json.JsonRestResponseHandler;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;

import java.io.IOException;
import java.io.OutputStream;

/**
 * @author Renze de Vries
 */
public class OutputHandler {
    private static final Logger LOG = LoggerFactory.getLogger(OutputHandler.class);

    private static final JsonRestResponseHandler restResponseHandler = new JsonRestResponseHandler();

    public static void createResponse(final RestEntity entity, HttpServletResponse response) {
        if(entity != null) {
            if(entity instanceof ErrorEntity) {
                handleError((ErrorEntity)entity, response);
            } else {
                sendResponse(entity, HttpStatus.OK.value(), response);
            }
        } else {
            handleError(new ErrorEntity(HttpStatus.NOT_FOUND.value(), "Resource could not be found"), response);
        }
    }

    public static void handleError(String message, HttpServletResponse response) {
        handleError(new ErrorEntity(HttpStatus.BAD_REQUEST.value(), message), response);
    }

    private static void handleError(ErrorEntity errorEntity, HttpServletResponse response)  {
        sendResponse(errorEntity, errorEntity.getStatusCode(), response);
    }

    public static RestResponseHandler getResponseHandler() {
        return restResponseHandler;
    }

    private static void sendResponse(final RestEntity entity, int statusCode, HttpServletResponse response) {
        response.setStatus(statusCode);
        response.setContentType(restResponseHandler.getMediaType());
        try {
            OutputStream outputStream = response.getOutputStream();
            restResponseHandler.serialize(entity, outputStream);

        } catch (IOException | RestException e) {
            LOG.debug("Stream error, full stack: {}", e);
            LOG.info("Could not stream entity: {}", e.getMessage());
        } catch(Throwable e) {
            LOG.error("Unable to stream entity", e);
        }
    }
}
