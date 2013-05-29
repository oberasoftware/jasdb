package nl.renarj.jasdb.core.storage;

import nl.renarj.jasdb.core.exceptions.JasDBStorageException;

import java.io.File;

/**
 * This factory is responsible for creating the record storage writer and opening for
 * persistence
 *
 * @author Renze de Vries
 */
public interface RecordWriterFactory {
    RecordWriter createWriter(File file) throws JasDBStorageException;
}
