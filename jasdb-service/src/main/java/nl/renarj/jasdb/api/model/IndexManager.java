package nl.renarj.jasdb.api.model;

import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.index.Index;
import nl.renarj.jasdb.index.search.CompositeIndexField;
import nl.renarj.jasdb.index.search.IndexField;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface IndexManager {

	void shutdownIndexes() throws JasDBStorageException;

    void flush(String bagName) throws JasDBStorageException;

    void flush() throws JasDBStorageException;

	List<Index> getLoadedIndexes();

	Index getBestMatchingIndex(String bagName, Set<String> fields) throws JasDBStorageException;

	Map<String, Index> getIndexes(String bagName) throws JasDBStorageException;

	Index getIndex(String bagName, String keyName) throws JasDBStorageException;

	Index createIndex(String bagName, CompositeIndexField compositeIndexFields, boolean unique, IndexField... values) throws JasDBStorageException;

	Index createIndex(String bagName, IndexField indexField, boolean unique, IndexField... valueFields) throws JasDBStorageException;

    void removeIndex(String bagName, String keyName) throws JasDBStorageException;
}