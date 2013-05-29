package nl.renarj.jasdb;

import nl.renarj.jasdb.core.SimpleKernel;
import nl.renarj.jasdb.core.exceptions.JasDBException;
import org.junit.After;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;

public class SimpleBaseTest {
    public static final String[] possibleCities = new String[] {"Amsterdam", "Rotterdam", "Utrecht", "Groningen", "Maastricht", "Breda", "Eindhoven", "Leiden", "Den Haag", "Haarlem"};
    private static final Logger LOG = LoggerFactory.getLogger(SimpleBaseTest.class);
	public static File tmpDir = new File(System.getProperty("java.io.tmpdir"));
    public static File jasdbDir = new File(tmpDir, ".jasdb");
	
	@After
	public void tearDown() throws JasDBException {
		SimpleKernel.shutdown();
	}
	
    public static void cleanData(File dir) {
        if(dir.exists()) {
            LOG.info("Starting cleaning data in directory: {}", dir);
            File[] cleanupFiles = dir.listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    return name.endsWith(".pjs") || name.endsWith(".idx") || name.endsWith(".idxm") || name.endsWith(".pjsm") || name.endsWith(".pid");
                }
            });
            LOG.info("Cleaning up {} files", cleanupFiles.length);
            if(cleanupFiles != null) {
                for(File cleanFile : cleanupFiles) {
                    boolean success = cleanFile.delete();
                    LOG.info("Deleting of file: {} successful: {}", cleanFile, success);
                }
            }
        }
    }

    public static void cleanData() {
        cleanData(tmpDir);
        cleanData(jasdbDir);
    }

}
