package nl.renarj.jasdb.index.btreeplus.search;

import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.index.IndexIterator;
import nl.renarj.jasdb.index.btreeplus.BlockPersister;
import nl.renarj.jasdb.index.btreeplus.FullIndexIterator;
import nl.renarj.jasdb.index.btreeplus.RootBlock;
import nl.renarj.jasdb.index.btreeplus.locking.LockManager;
import nl.renarj.jasdb.index.keys.CompareMethod;
import nl.renarj.jasdb.index.keys.Key;
import nl.renarj.jasdb.index.keys.keyinfo.KeyInfo;
import nl.renarj.jasdb.index.result.IndexSearchResultIteratorCollection;
import nl.renarj.jasdb.index.result.IndexSearchResultIteratorImpl;
import nl.renarj.jasdb.index.result.SearchLimit;
import nl.renarj.jasdb.index.search.EqualsCondition;
import nl.renarj.jasdb.index.search.SearchCondition;

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

        List<Key> results = new LinkedList<Key>();
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
