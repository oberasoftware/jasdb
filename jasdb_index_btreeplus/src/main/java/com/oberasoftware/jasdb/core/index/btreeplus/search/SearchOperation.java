package com.oberasoftware.jasdb.core.index.btreeplus.search;

import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.index.query.IndexSearchResultIteratorCollection;
import com.oberasoftware.jasdb.api.index.query.SearchLimit;
import com.oberasoftware.jasdb.api.index.query.SearchCondition;

/**
 * @author Renze de Vries
 */
public interface SearchOperation {
    IndexSearchResultIteratorCollection search(SearchCondition condition, SearchLimit limit) throws JasDBStorageException;
}
