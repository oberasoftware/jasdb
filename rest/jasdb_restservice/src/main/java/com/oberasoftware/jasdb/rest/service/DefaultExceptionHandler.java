package com.oberasoftware.jasdb.rest.service;

import com.oberasoftware.jasdb.rest.model.ErrorEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;

import javax.servlet.http.HttpServletRequest;

/**
 * @author Renze de Vries
 */
@ControllerAdvice
public class DefaultExceptionHandler {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultExceptionHandler.class);

    public static final String DEFAULT_ERROR_VIEW = "error";

    @ExceptionHandler(value = Exception.class)
    public ResponseEntity<ErrorEntity> defaultErrorHandler(HttpServletRequest req, Exception e) throws Exception {
        LOG.error("Intercepted exception: {} for URL: {}", e.getMessage(), req.getRequestURL());
        LOG.error("Exception stacktrace:", e);

        if (AnnotationUtils.findAnnotation(e.getClass(), ResponseStatus.class) != null) {
            throw e;
        }

        return new ResponseEntity<>(new ErrorEntity(500, e.getMessage()), HttpStatus.INTERNAL_SERVER_ERROR);
    }


}
