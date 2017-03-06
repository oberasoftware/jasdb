package nl.renarj.jasdb.localservice.api;

import com.oberasoftware.jasdb.service.local.LocalDBSessionFactory;
import com.oberasoftware.jasdb.engine.TableScanQueryTest;

/**
 * @author Renze de Vries
 */
public class LocalTableScanQueryTest extends TableScanQueryTest {
    public LocalTableScanQueryTest() {
        super(new LocalDBSessionFactory());
    }

}
