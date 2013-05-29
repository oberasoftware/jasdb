package nl.renarj.jasdb.localservice.api;

import nl.renarj.jasdb.LocalDBSessionFactory;
import nl.renarj.jasdb.service.DBSessionTest;

/**
 * @author Renze de Vries
 */
public class LocalDBSessionTest extends DBSessionTest {
    public LocalDBSessionTest() {
        super(new LocalDBSessionFactory());
    }
}
