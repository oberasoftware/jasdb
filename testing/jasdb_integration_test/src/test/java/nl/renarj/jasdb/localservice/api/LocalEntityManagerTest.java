package nl.renarj.jasdb.localservice.api;

import nl.renarj.jasdb.LocalDBSessionFactory;
import nl.renarj.jasdb.service.EntityManagerTest;

/**
 * @author Renze de Vries
 */
public class LocalEntityManagerTest extends EntityManagerTest {
    public LocalEntityManagerTest() {
        super(new LocalDBSessionFactory());
    }
}
