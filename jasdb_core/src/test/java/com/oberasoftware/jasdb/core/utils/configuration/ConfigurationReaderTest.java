package com.oberasoftware.jasdb.core.utils.configuration;

import com.oberasoftware.jasdb.api.engine.Configuration;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ConfigurationReaderTest {
	@Test
	public void testReadConfiguration() throws Exception {
		Configuration config = XMLConfiguration.loadConfiguration("some_config.xml");
		assertNotNull(config);
		
		List<Configuration> configs = config.getChildConfigurations("/pojodb/datastore/subchild[@myattr='test']");
		assertNotNull(configs);
		assertEquals("There should only be one config element found", 1, configs.size());
		
		Configuration foundConfig = config.getChildConfiguration("/pojodb/datastore/subchild[@myattr='test']");
		assertNotNull(foundConfig);
		
		String attr = foundConfig.getAttribute("myattr");
		assertEquals("Attribute value should be 'test'", "test", attr);
	}
}
