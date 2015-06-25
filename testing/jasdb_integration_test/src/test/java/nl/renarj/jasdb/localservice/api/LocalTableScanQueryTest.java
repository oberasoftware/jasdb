package nl.renarj.jasdb.localservice.api;

import nl.renarj.jasdb.LocalDBSessionFactory;
import nl.renarj.jasdb.service.TableScanQueryTest;

/**
 * @author Renze de Vries
 */
public class LocalTableScanQueryTest extends TableScanQueryTest {
    public LocalTableScanQueryTest() {
        super(new LocalDBSessionFactory());
    }

}
