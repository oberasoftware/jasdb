/*
 * The JASDB software and code is Copyright protected 2012 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2012 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package com.oberasoftware.jasdb.core.index.btreeplus.search;

import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.core.index.btreeplus.LeaveBlock;
import com.oberasoftware.jasdb.core.index.btreeplus.RootBlock;
import com.oberasoftware.jasdb.core.index.btreeplus.locking.LockIntentType;
import com.oberasoftware.jasdb.core.index.btreeplus.locking.LockManager;
import com.oberasoftware.jasdb.api.index.keys.Key;
import com.oberasoftware.jasdb.api.index.keys.KeyFactory;
import com.oberasoftware.jasdb.api.index.keys.KeyInfo;
import com.oberasoftware.jasdb.api.index.query.IndexSearchResultIteratorCollection;
import com.oberasoftware.jasdb.core.index.query.IndexSearchResultIteratorImpl;
import com.oberasoftware.jasdb.api.index.query.SearchLimit;
import com.oberasoftware.jasdb.core.index.query.EqualsCondition;
import com.oberasoftware.jasdb.api.index.query.SearchCondition;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Renze de Vries
 */
public class EqualsSearchOperation implements SearchOperation {
    private RootBlock rootBlock;
    private KeyInfo keyInfo;
    private LockManager lockManager;

    public EqualsSearchOperation(LockManager lockManager, RootBlock rootBlock, KeyInfo keyInfo) {
        this.rootBlock = rootBlock;
        this.keyInfo = keyInfo;
        this.lockManager = lockManager;
    }

    @Override
    public IndexSearchResultIteratorCollection search(SearchCondition condition, SearchLimit limit) throws JasDBStorageException {
        EqualsCondition equalsCondition = validateCondition(keyInfo, condition);
        Key desiredKey = equalsCondition.getKey();

        lockManager.startLockChain();
        lockManager.acquireLock(LockIntentType.READ, rootBlock);
        try {
            LeaveBlock leaveBlock = rootBlock.findLeaveBlock(LockIntentType.READ, equalsCondition.getKey());
            return doLeaveSearch(leaveBlock, desiredKey);
        } finally {
            lockManager.releaseLockChain();
        }
    }

    private IndexSearchResultIteratorCollection doLeaveSearch(LeaveBlock leaveBlock, Key desiredKey) {
        List<Key> results = new ArrayList<>(1);

        if(leaveBlock.contains(desiredKey)) {
            results.add(leaveBlock.getKey(desiredKey));
        }

        return new IndexSearchResultIteratorImpl(results, keyInfo.getKeyNameMapper().clone());
    }

    protected static EqualsCondition validateCondition(KeyInfo keyInfo, SearchCondition condition) throws JasDBStorageException {
        if(condition instanceof EqualsCondition) {
            EqualsCondition equalsCondition = (EqualsCondition) condition;
            KeyFactory keyFactory = keyInfo.getKeyFactory();
            if(!keyFactory.supportsKey(equalsCondition.getKey())) {
                Key supportedKey = keyFactory.convertKey(equalsCondition.getKey());

                return new EqualsCondition(supportedKey);
            } else {
                return equalsCondition;
            }
        } else {
            throw new JasDBStorageException("Invalid Equals condition input: " + condition);
        }
    }

}
