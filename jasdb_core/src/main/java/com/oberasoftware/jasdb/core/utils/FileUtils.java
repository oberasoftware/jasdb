package com.oberasoftware.jasdb.core.utils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;

/**
 * @author Renze de Vries
 */
public class FileUtils {
    public static final String DEFAULT_ENCODING = "UTF8";

    public static void writeToFile(File file, String contents) throws IOException {
        FileOutputStream out = new FileOutputStream(file);
        out.write(contents.getBytes(DEFAULT_ENCODING));
        out.close();
    }

    /**
     * Attempts to delete a file, if fails tries again on exit
     * @param file The file to delete
     */
    public static void deleteSafely(File file) {
        if(file.exists()) {
            if (!file.delete()) {
                file.deleteOnExit();
            }
        }
    }

    /**
     * Returns the string minus the extension
     * @param fileName The filename
     * @return The filename without the extension
     */
    public static String removeExtension(String fileName) {
        int extensionIndex = fileName.lastIndexOf('.');

        return (extensionIndex > 0) ? fileName.substring(0, extensionIndex) : fileName;
    }

    public static String resolveResourcePath(String resourcePath) {
        if(resourcePath.startsWith("classpath:")) {
            String strippedPath = resourcePath.replace("classpath:", "");
            return loadResource(strippedPath).toString();
        } else {
            return resourcePath;
        }
    }

    public static URL loadResource(String resourcePath) {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        return classLoader.getResource(resourcePath);
    }
}
