package nl.renarj.jasdb.localservice.api;

import com.oberasoftware.jasdb.service.local.LocalDBSessionFactory;
import com.oberasoftware.jasdb.engine.EntityManagerTest;

/**
 * @author Renze de Vries
 */
public class LocalEntityManagerTest extends EntityManagerTest {
    public LocalEntityManagerTest() {
        super(new LocalDBSessionFactory());
    }
}
