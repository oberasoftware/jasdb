package com.oberasoftware.jasdb.api.storage;

import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;

import java.io.File;

/**
 * This factory is responsible for creating the record storage writer and opening for
 * persistence
 *
 * @author Renze de Vries
 */
public interface RecordWriterFactory<T> {
    /**
     * Provides the provides name for this factory
     * @return The name of the provider
     */
    String providerName();

    /**
     * Creates a record writer for the given file location
     * @param file The file on which to create a record writer
     * @return The record writer
     * @throws JasDBStorageException If unable to create a record writer
     */
    RecordWriter<T> createWriter(File file) throws JasDBStorageException;
}
