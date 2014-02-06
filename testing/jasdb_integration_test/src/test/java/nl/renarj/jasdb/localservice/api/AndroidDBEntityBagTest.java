package nl.renarj.jasdb.localservice.api;

import com.obera.jasdb.android.platform.AndroidPlatformManager;
import nl.renarj.jasdb.core.platform.PlatformManagerFactory;
import nl.renarj.jasdb.service.EntityBagTest;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Renze de Vries
 */
public class AndroidDBEntityBagTest extends EntityBagTest {
    private static final Logger LOG = LoggerFactory.getLogger(AndroidDBEntityBagTest.class);

    public AndroidDBEntityBagTest() {
        super(new AndroidDBSessionFactory());
    }

    @Before
    public void setUp() throws Exception {
        System.setProperty("jasdb.platform.manager", AndroidPlatformManager.class.toString());
        super.setUp();
    }



    @After
    public void tearDown() throws Exception {
        super.tearDown();

        System.setProperty("jasdb.platform.manager", "");
        PlatformManagerFactory.setPlatformManager(null);

        LOG.info("Platform manager reset");
    }
}
