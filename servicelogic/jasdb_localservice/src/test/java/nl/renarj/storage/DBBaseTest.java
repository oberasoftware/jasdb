package nl.renarj.storage;

import junit.framework.Assert;
import nl.renarj.jasdb.core.SimpleKernel;
import nl.renarj.jasdb.core.exceptions.JasDBException;
import org.junit.After;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;

public abstract class DBBaseTest {
	public static File tmpDir = new File(System.getProperty("java.io.tmpdir"));
    public static File jasdbDir = new File(tmpDir, ".jasdb");

	private static final Logger log = LoggerFactory.getLogger(DBBaseTest.class);

	@After
	public void tearDown() throws JasDBException {
		SimpleKernel.shutdown();
	}
	
	protected static void assertDelete(File deleteFile) {
		if(deleteFile.exists()) {
			Assert.assertTrue(deleteFile.delete());
		}
	}
	
	protected static void assertFileExists(File file, boolean shouldExist) {
		Assert.assertEquals("File " + file.toString() + " should exist: " + shouldExist, shouldExist, file.exists());
	}
	
    public static void cleanData() {
        cleanData(tmpDir);
        cleanData(jasdbDir);
    }

    private static void cleanData(File folder) {
        log.info("Starting cleaning data in directory: {}", folder);
        File[] cleanupFiles = folder.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".pjs") || name.endsWith(".idx") || name.endsWith(".idxm");
            }
        });
        if(cleanupFiles != null) {
            log.info("Cleaning up {} files", cleanupFiles.length);
            for(File cleanFile : cleanupFiles) {
                boolean success = cleanFile.delete();
                log.info("Deleting of file: {} successful: {}", cleanFile, success);
            }
        }

    }

}
