package com.oberasoftware.jasdb.core.utils.configuration;

import java.util.HashMap;

public class ConfigurationProperty extends ManualConfiguration {
	public ConfigurationProperty(String propertyName, String value) {
		super("Property", new HashMap<String, String>());
		this.addAttribute("Name", propertyName);
		this.addAttribute("Value", value);
	}
}
