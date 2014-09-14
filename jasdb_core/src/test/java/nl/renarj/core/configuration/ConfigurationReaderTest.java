package nl.renarj.core.configuration;

import junit.framework.Assert;
import nl.renarj.core.utilities.configuration.Configuration;
import org.junit.Test;

import java.util.List;

public class ConfigurationReaderTest {
	@Test
	public void testReadConfiguration() throws Exception {
		Configuration config = Configuration.loadConfiguration("some_config.xml");
		Assert.assertNotNull(config);
		
		List<Configuration> configs = config.getChildConfigurations("/pojodb/datastore/subchild[@myattr='test']");
		Assert.assertNotNull(configs);
		Assert.assertEquals("There should only be one config element found", 1, configs.size());
		
		Configuration foundConfig = config.getChildConfiguration("/pojodb/datastore/subchild[@myattr='test']");
		Assert.assertNotNull(foundConfig);
		
		String attr = foundConfig.getAttribute("myattr");
		Assert.assertEquals("Attribute value should be 'test'", "test", attr);
	}
}
