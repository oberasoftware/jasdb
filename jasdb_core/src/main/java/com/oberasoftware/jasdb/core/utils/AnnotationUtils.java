package com.oberasoftware.jasdb.core.utils;

import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.util.Optional;

/**
 * @author Renze de Vries
 */
public class AnnotationUtils {
    private AnnotationUtils() {

    }

    public static <T extends Annotation> T getAnnotation(AnnotatedElement type, Class<T> annotationType) throws JasDBStorageException {
        Optional<T> annotation = getOptionalAnnotation(type, annotationType);
        if(annotation.isPresent()) {
            return annotation.get();
        } else {
            throw new JasDBStorageException("Unable to get annotation: " + annotationType);
        }

    }



    public static <T extends Annotation> Optional<T> getOptionalAnnotation(AnnotatedElement type, Class<T> annotationType) {
        T annotation = type.getAnnotation(annotationType);
        return Optional.ofNullable(annotation);
    }

}
