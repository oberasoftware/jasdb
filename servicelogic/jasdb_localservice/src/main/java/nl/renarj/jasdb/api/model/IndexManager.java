package nl.renarj.jasdb.api.model;

import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.index.Index;
import nl.renarj.jasdb.index.search.CompositeIndexField;
import nl.renarj.jasdb.index.search.IndexField;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface IndexManager {

	public void shutdownIndexes() throws JasDBStorageException;

	public List<Index> getLoadedIndexes();

	public Index getBestMatchingIndex(String bagName, Set<String> fields) throws JasDBStorageException;

	public Map<String, Index> getIndexes(String bagName) throws JasDBStorageException;

	public Index getIndex(String bagName, String keyName) throws JasDBStorageException;

	public Index createIndex(String bagName, CompositeIndexField compositeIndexFields, boolean unique, IndexField... values) throws JasDBStorageException;

	public Index createIndex(String bagName, IndexField indexField,	boolean unique, IndexField... valueFields) throws JasDBStorageException;

    public void removeIndex(String bagName, String keyName) throws JasDBStorageException;
}