package com.oberasoftware.jasdb.integration;

import com.oberasoftware.jasdb.service.local.LocalDBSessionFactory;
import com.oberasoftware.jasdb.test.SortAndLimitQueryTest;

/**
 * @author Renze de Vries
 */
public class LocalSortAndLimitQueryTest extends SortAndLimitQueryTest {
    public LocalSortAndLimitQueryTest() {
        super(new LocalDBSessionFactory());
    }
}
