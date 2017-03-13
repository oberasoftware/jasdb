package com.oberasoftware.jasdb.core.utils;

import com.oberasoftware.jasdb.api.exceptions.FileException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

public class ResourceUtil {
	private static final Logger LOG = LoggerFactory.getLogger(ResourceUtil.class);
	
	public static String getContent(String resource, String encoding) throws FileException {
		InputStream systemResource = ClassLoader.getSystemClassLoader().getResourceAsStream(resource);

		if(systemResource != null) {
			try {
				byte[] buffer = new byte[4096];
				
				StringBuilder builder = new StringBuilder();
				int read = -1;
				while((read = systemResource.read(buffer)) != -1) {
					builder.append(new String(buffer, 0, read, encoding));
				}
				
				return builder.toString();
			} catch(IOException e) {
				throw new FileException("Unable to read system resource", e);
			} finally {
				try {
					systemResource.close();
				} catch(IOException e) {
					LOG.error("Unable to cleanly close system input resource", e);
				}
			}
		} else {
			return null;
		}
		
	}
}
