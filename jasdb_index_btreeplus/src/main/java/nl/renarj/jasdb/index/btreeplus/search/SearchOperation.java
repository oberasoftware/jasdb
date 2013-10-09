package nl.renarj.jasdb.index.btreeplus.search;

import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.index.result.IndexSearchResultIteratorCollection;
import nl.renarj.jasdb.index.result.SearchLimit;
import nl.renarj.jasdb.index.search.SearchCondition;

/**
 * @author Renze de Vries
 */
public interface SearchOperation {
    IndexSearchResultIteratorCollection search(SearchCondition condition, SearchLimit limit) throws JasDBStorageException;
}
