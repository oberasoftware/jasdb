package nl.renarj.jasdb.storage.query.operators.mergers;

import nl.renarj.jasdb.index.keys.Key;
import nl.renarj.jasdb.index.result.IndexSearchResultIteratorCollection;
import nl.renarj.jasdb.index.result.IndexSearchResultIteratorImpl;
import nl.renarj.jasdb.storage.query.operators.BlockMerger;

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
		List<Key> mergedKeys = new ArrayList<Key>(mergeInto.getKeys()); 
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
