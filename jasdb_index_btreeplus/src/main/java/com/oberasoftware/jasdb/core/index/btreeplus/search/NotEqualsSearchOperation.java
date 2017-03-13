package com.oberasoftware.jasdb.core.index.btreeplus.search;

import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.index.IndexIterator;
import com.oberasoftware.jasdb.core.index.btreeplus.BlockPersister;
import com.oberasoftware.jasdb.core.index.btreeplus.FullIndexIterator;
import com.oberasoftware.jasdb.core.index.btreeplus.RootBlock;
import com.oberasoftware.jasdb.core.index.btreeplus.locking.LockManager;
import com.oberasoftware.jasdb.api.index.keys.CompareMethod;
import com.oberasoftware.jasdb.api.index.keys.Key;
import com.oberasoftware.jasdb.api.index.keys.KeyInfo;
import com.oberasoftware.jasdb.api.index.query.IndexSearchResultIteratorCollection;
import com.oberasoftware.jasdb.core.index.query.IndexSearchResultIteratorImpl;
import com.oberasoftware.jasdb.api.index.query.SearchLimit;
import com.oberasoftware.jasdb.core.index.query.EqualsCondition;
import com.oberasoftware.jasdb.api.index.query.SearchCondition;

import java.util.LinkedList;
import java.util.List;

/**
 * @author Renze de Vries
 */
public class NotEqualsSearchOperation implements SearchOperation {
    private LockManager lockManager;
    private RootBlock rootBlock;
    private KeyInfo keyInfo;
    private BlockPersister blockPersister;

    public NotEqualsSearchOperation(LockManager lockManager, KeyInfo keyInfo, RootBlock rootBlock, BlockPersister persister) {
        this.lockManager = lockManager;
        this.rootBlock = rootBlock;
        this.keyInfo = keyInfo;
        this.blockPersister = persister;
    }

    @Override
    public IndexSearchResultIteratorCollection search(SearchCondition condition, SearchLimit limit) throws JasDBStorageException {
        IndexIterator indexIterator = new FullIndexIterator(rootBlock, lockManager, blockPersister);

        EqualsCondition equalsCondition = EqualsSearchOperation.validateCondition(keyInfo, condition);
        Key undesiredKey = equalsCondition.getKey();

        List<Key> results = new LinkedList<>();
        for(Key key : indexIterator) {
            if(key.compare(undesiredKey, CompareMethod.EQUALS).getCompare() != 0) {
                results.add(key);
            }

            if(limit.isMaxReached(results.size())) {
                break;
            }
        }

        return new IndexSearchResultIteratorImpl(results, keyInfo.getKeyNameMapper());
    }
}
