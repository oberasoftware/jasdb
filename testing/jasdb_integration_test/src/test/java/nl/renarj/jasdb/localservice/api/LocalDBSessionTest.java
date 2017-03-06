package nl.renarj.jasdb.localservice.api;

import com.oberasoftware.jasdb.service.local.LocalDBSessionFactory;
import com.oberasoftware.jasdb.engine.DBSessionTest;

/**
 * @author Renze de Vries
 */
public class LocalDBSessionTest extends DBSessionTest {
    public LocalDBSessionTest() {
        super(new LocalDBSessionFactory());
    }
}
