/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package com.oberasoftware.jasdb.core.index.query;

import com.oberasoftware.jasdb.api.index.IndexField;
import com.oberasoftware.jasdb.api.index.CompositeIndexField;

import java.util.Arrays;
import java.util.List;

/**
 * The composite index field is used to create a composite index which
 * can contain a number of keys
 */
public class SimpleCompositeIndexField implements CompositeIndexField {
	private List<IndexField> indexFields;

    /**
     * Creates the composite key for the provided fields
     * @param fields The fields to add to the composite key
     */
	public SimpleCompositeIndexField(IndexField... fields) {
		this.indexFields = Arrays.asList(fields);
	}

	@Override
    public List<IndexField> getIndexFields() {
		return indexFields;
	}
}
