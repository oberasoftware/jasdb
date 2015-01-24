package nl.renarj.core.utilities.configuration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 
 * @author Renze de Vries
 *
 */
public class ManualConfiguration extends Configuration {
	private Map<String, String> attributes;
	private Map<String, List<Configuration>> childConfigs;
	private String name;
	
	public ManualConfiguration(String name, Map<String, String> attributes) {
		super(null);
		this.name = name;
		this.attributes = new ConcurrentHashMap<String, String>(attributes);
		this.childConfigs = new ConcurrentHashMap<String, List<Configuration>>();
	}
	
	public String getName() {
		return this.name;
	}
	
	protected void addAttribute(String name, String value) {
		this.attributes.put(name, value);
	}
	
	public void addChildConfiguration(String name, Configuration childConfiguration) {
		if(!this.childConfigs.containsKey(name)) {
			this.childConfigs.put(name, new ArrayList<Configuration>());
		}
		List<Configuration> childConfigList = childConfigs.get(name);
		childConfigList.add(childConfiguration);
	}

	@Override
	public String getAttribute(String name, String defaultValue) {
		if(attributes.containsKey(name)) {
			return attributes.get(name);
		} else {
			return defaultValue;
		}
	}

    @Override
    public int getAttribute(String attributeName, int defaultValue) {
        if(attributes.containsKey(attributeName)) {
            String numberValue = attributes.get(attributeName);
            try {
                return Integer.parseInt(numberValue);
            } catch(NumberFormatException e) {
                return defaultValue;
            }
        }
        return defaultValue;
    }

    @Override
	public String getAttribute(String attributeName) {
		return this.attributes.get(attributeName);
	}

	@Override
	public Configuration getChildConfiguration(String arg0) {
		List<Configuration> configs = this.childConfigs.get(arg0);
		if(configs != null && !configs.isEmpty()) {
			return configs.get(0);
		} else {
			return null;
		}
	}

	@Override
	public List<Configuration> getChildConfigurations(String arg0) {
		if(childConfigs.containsKey(arg0)) {
			return new ArrayList<Configuration>(childConfigs.get(arg0));
		} else {
			return Collections.emptyList();
		}
	}
}