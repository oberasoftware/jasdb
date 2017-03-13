/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package com.oberasoftware.jasdb.core.index.keys.factory;

import com.oberasoftware.jasdb.api.exceptions.CoreConfigException;
import com.oberasoftware.jasdb.api.exceptions.ReflectionException;
import com.oberasoftware.jasdb.api.index.keys.KeyFactory;
import com.oberasoftware.jasdb.core.utils.ReflectionLoader;
import com.oberasoftware.jasdb.core.utils.StringUtils;
import com.oberasoftware.jasdb.api.engine.Configuration;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.core.index.keys.keyinfo.KeyNameMapperImpl;
import com.oberasoftware.jasdb.core.index.keys.keyinfo.MultiKeyLoaderImpl;
import com.oberasoftware.jasdb.api.index.keys.KeyType;
import com.oberasoftware.jasdb.core.utils.configuration.XMLConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class KeyFactoryManager {
	private static final Logger LOG = LoggerFactory.getLogger(KeyFactoryManager.class);
    private static final Pattern KEY_PATTERN = Pattern.compile("([\\p{L}\\p{N} _]+)\\(([\\w:]+)\\)|([\\p{L} ]+)\\((.*)\\)");
	
	private static KeyFactoryManager instance;
	private Map<String, KeyFactoryDefinition> keyFactories;
	
	private KeyFactoryManager(Map<String, KeyFactoryDefinition> keyFactories) {
		this.keyFactories = keyFactories;
	}
	
	private static KeyFactoryManager getInstance() {
		if(instance == null) {
			loadKeyFactories();
		}
		return instance;
	}
	
	private static synchronized void loadKeyFactories() {
		try {
			Map<String, KeyFactoryDefinition> definitions = new HashMap<>();
			Configuration keyFactoriesConfig = XMLConfiguration.loadConfiguration("db_keyfactories.xml");
			List<Configuration> keyFactoryConfigs = keyFactoriesConfig.getChildConfigurations("/keyFactories/keyFactory");
			for(Configuration keyFactoryConfig : keyFactoryConfigs) {
				String keyFactoryClass = keyFactoryConfig.getAttribute("class");
				String keyFactoryId = keyFactoryConfig.getAttribute("id");
				
				KeyFactoryDefinition definition = new KeyFactoryDefinition(keyFactoryClass, keyFactoryId);
				definitions.put(keyFactoryId, definition);
			}
			instance = new KeyFactoryManager(definitions);
		} catch(CoreConfigException e) {
			LOG.error("unable to load key factories", e);
		}
	}
	
	private KeyFactoryDefinition getKeyFactory(String id) {
		return keyFactories.get(id);
	}

    public static KeyFactory[] parseHeader(String headerDescriptor) throws JasDBStorageException {
        LOG.debug("Parsing header: {}", headerDescriptor);
        if(StringUtils.stringNotEmpty(headerDescriptor)) {
            Matcher matcher = KEY_PATTERN.matcher(headerDescriptor);

            List<KeyFactory> keyFactoryList = new ArrayList<>();
            while(matcher.find()) {
                String keyName = matcher.group(1) != null ? matcher.group(1) : matcher.group(3);
                String keyValue = matcher.group(2) != null ? matcher.group(2) : matcher.group(4);

                if("complexType".equals(keyName)) {
                    LOG.debug("Found complex type: {}", keyValue);
                    KeyFactory[] compositeKeyFactories = parseHeader(keyValue);

                    keyFactoryList.add(new CompositeKeyFactory(new MultiKeyLoaderImpl(KeyNameMapperImpl.create(compositeKeyFactories), compositeKeyFactories)));
                } else {
                    String[] arguments = keyValue.split(":");
                    String fieldType = arguments[0];
                    String[] keyArguments = new String[0];
                    if(arguments.length > 1) {
                        keyArguments = Arrays.copyOfRange(arguments, 1, arguments.length);
                    }
                    keyFactoryList.add(KeyFactoryManager.createKeyFactory(fieldType, keyName, keyArguments));
                }

                LOG.debug("Found field: {} with type info: {}", keyName, keyValue);
            }
            return keyFactoryList.toArray(new KeyFactory[keyFactoryList.size()]);
        } else {
            return new KeyFactory[0];
        }
    }

    public static KeyFactory createKeyFactory(String field, KeyType keyType) throws JasDBStorageException {
        return createKeyFactory(keyType.getKeyId(), field, keyType.getKeyArguments());
    }

    private static KeyFactory createKeyFactory(String keyId, String field, String[] keyArgs) throws JasDBStorageException {
		KeyFactoryDefinition definition = getInstance().getKeyFactory(keyId);
		if(definition != null) {
			LOG.debug("Loaded keyFactory: {} for id: {}", definition.getKeyFactoryClass(),
					definition.getKeyFactoryId());
			LOG.debug("Using arguments: {}", (Object[]) keyArgs);
			
			List<String> arguments = new ArrayList<>();
			arguments.add(field);
			arguments.addAll(Arrays.asList(keyArgs));
			
			KeyFactory keyFactory = loadKeyFactory(definition.getKeyFactoryClass(), arguments.toArray(new String[arguments.size()]));
			if(keyFactory != null) {
				return keyFactory;
			} 
		} 
		
		throw new JasDBStorageException("Unable to load keyFactory for KeyId: " + keyId);
	}
	
	private static KeyFactory loadKeyFactory(String keyFactoryClass, String[] keyArgs) {
		try {
			return ReflectionLoader.loadClass(KeyFactory.class, keyFactoryClass, keyArgs);
		} catch(ReflectionException e) {
			LOG.error("Unable to load KeyFactory: {}", e.getMessage());
			return null;
		}
	}
	
	private static final class KeyFactoryDefinition {
		private String keyFactoryClass;
		private String keyFactoryId;
		
		private KeyFactoryDefinition(String keyFactoryClass, String id) {
			this.keyFactoryClass = keyFactoryClass;
			this.keyFactoryId = id;
		}

		public String getKeyFactoryClass() {
			return keyFactoryClass;
		}

		public String getKeyFactoryId() {
			return keyFactoryId;
		}
	}
}
