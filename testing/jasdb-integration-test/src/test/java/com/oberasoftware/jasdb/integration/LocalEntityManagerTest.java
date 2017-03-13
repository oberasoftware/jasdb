package com.oberasoftware.jasdb.integration;

import com.oberasoftware.jasdb.service.local.LocalDBSessionFactory;
import com.oberasoftware.jasdb.test.EntityManagerTest;

/**
 * @author Renze de Vries
 */
public class LocalEntityManagerTest extends EntityManagerTest {
    public LocalEntityManagerTest() {
        super(new LocalDBSessionFactory());
    }
}
