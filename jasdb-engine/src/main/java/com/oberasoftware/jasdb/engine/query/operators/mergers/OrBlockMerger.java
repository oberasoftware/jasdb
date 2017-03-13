package com.oberasoftware.jasdb.engine.query.operators.mergers;

import com.oberasoftware.jasdb.api.index.keys.Key;
import com.oberasoftware.jasdb.api.index.query.IndexSearchResultIteratorCollection;
import com.oberasoftware.jasdb.core.index.query.IndexSearchResultIteratorImpl;
import com.oberasoftware.jasdb.engine.query.operators.BlockMerger;

import java.util.ArrayList;
import java.util.List;

public class OrBlockMerger implements BlockMerger {
	@Override
	public boolean includeResult(boolean leftResultFound, boolean rightResultFound) {
		return leftResultFound || rightResultFound;
	}

    @Override
    public boolean continueEvaluation(boolean currentState) {
        return true;
    }

    @Override
	public IndexSearchResultIteratorCollection mergeIterators(IndexSearchResultIteratorCollection mergeInto, IndexSearchResultIteratorCollection... results) {
		List<Key> mergedKeys = new ArrayList<>(mergeInto.getKeys());
		for(IndexSearchResultIteratorCollection collection : results) {
			mergeInto(mergedKeys, collection);
		}

		return new IndexSearchResultIteratorImpl(mergedKeys, mergeInto.getKeyNameMapper());
	}
	
	private void mergeInto(List<Key> mergeInto, IndexSearchResultIteratorCollection from) {
		for(Key key : from) {
			mergeInto.add(key);
		}
	}
}
