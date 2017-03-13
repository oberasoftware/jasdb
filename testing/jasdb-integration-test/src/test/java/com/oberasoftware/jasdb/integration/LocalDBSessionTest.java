package com.oberasoftware.jasdb.integration;

import com.oberasoftware.jasdb.service.local.LocalDBSessionFactory;
import com.oberasoftware.jasdb.test.DBSessionTest;

/**
 * @author Renze de Vries
 */
public class LocalDBSessionTest extends DBSessionTest {
    public LocalDBSessionTest() {
        super(new LocalDBSessionFactory());
    }
}
