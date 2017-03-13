package com.oberasoftware.jasdb.integration;

import com.oberasoftware.jasdb.service.local.LocalDBSessionFactory;
import com.oberasoftware.jasdb.test.TableScanQueryTest;

/**
 * @author Renze de Vries
 */
public class LocalTableScanQueryTest extends TableScanQueryTest {
    public LocalTableScanQueryTest() {
        super(new LocalDBSessionFactory());
    }

}
