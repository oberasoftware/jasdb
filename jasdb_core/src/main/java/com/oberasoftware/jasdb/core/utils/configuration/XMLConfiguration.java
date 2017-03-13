package com.oberasoftware.jasdb.core.utils.configuration;

import com.oberasoftware.jasdb.api.engine.Configuration;
import com.oberasoftware.jasdb.api.exceptions.CoreConfigException;
import com.oberasoftware.jasdb.core.utils.XMLReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class XMLConfiguration implements Configuration {
	private static final Logger LOG = LoggerFactory.getLogger(XMLConfiguration.class);
	
	private Node configurationNode;
	
	protected XMLConfiguration(Node configurationNode) {
		this.configurationNode = configurationNode;
	}

	public static Configuration loadConfiguration(String fileName) throws CoreConfigException {
		LOG.info("Loading configuration: {}", fileName);
		Document document = XMLReader.getDocument(fileName);

		return new XMLConfiguration(document.getDocumentElement());
	}

	@Override
    public String getAttribute(String attributeName) {
		return getAttribute(attributeName, null);
	}
	
	@Override
    public String getAttribute(String attributeName, String defaultValue) {
		try {
			XPath xpath = getXpath();
			
			String result = (String) xpath.evaluate("./@" + attributeName, this.configurationNode, XPathConstants.STRING);
			if(result != null && !result.isEmpty()) {
				return result;
			} else {
				return defaultValue;
			}
		} catch(XPathExpressionException e) {
			LOG.error("Unable to find the value for attribute: " + attributeName, e.getMessage());
			return defaultValue;
		}
	}
	
	@Override
    public int getAttribute(String attributeName, int defaultValue) {
		String value = getAttribute(attributeName, null);
		if(value != null) {
			try {
				return Integer.parseInt(value);
			} catch(NumberFormatException e) {
				LOG.error("Unable to parse configuration attribute: {} with value: {}", attributeName, value);
			}
		}
		
		return defaultValue;
	}
	
	@Override
    public boolean getAttribute(String attributeName, boolean defaultValue) {
		String value = getAttribute(attributeName, null);
		if(value != null) {
			return Boolean.valueOf(value) ? true : defaultValue;
		} else {
			return defaultValue;
		}
	}
	
	@Override
    public boolean hasAttribute(String attributeName) {
		return getAttribute(attributeName) != null ? true : false;
	}
	
	@Override
    public String getName() {
		return this.configurationNode.getNodeName();
	}
	
	@Override
    public Configuration getChildConfiguration(String configurationPath) {
		try {
			XPath xpath = getXpath();
			
			Node foundNode = (Node) xpath.evaluate(configurationPath, this.configurationNode, XPathConstants.NODE);
			if(foundNode != null) {
				return new XMLConfiguration(foundNode);
			}
		} catch(XPathExpressionException e) {
			LOG.error("Unable to find the configuration for path: " + configurationPath, e.getMessage());
		}
		
		return null;
	}
	
	@Override
    public List<Configuration> getChildConfigurations(String configurationPath) {
		try {
			XPath xpath = getXpath();
			NodeList foundNodes = (NodeList) xpath.evaluate(configurationPath, this.configurationNode, XPathConstants.NODESET);
			
			return convertNodeList(foundNodes);
		} catch(XPathExpressionException e) {
			LOG.error("Unable to find the child configurations for path: " + configurationPath, e.getMessage());
		}
		
		return Collections.emptyList();
	}
	
	@Override
    public List<Configuration> getChildren() {
		return convertNodeList(configurationNode.getChildNodes());
	}
	
	private List<Configuration> convertNodeList(NodeList nodeList) {
		List<Configuration> loadedConfigurations = new ArrayList<Configuration>();
		for(int i=0; i<nodeList.getLength(); i++) {
			if(nodeList.item(i) instanceof Element) {
				loadedConfigurations.add(new XMLConfiguration(nodeList.item(i)));
			}
		}

		return loadedConfigurations;
	}
	
	private XPath getXpath() {
		XPathFactory xpathFactory = XPathFactory.newInstance();
		XPath xpath = xpathFactory.newXPath();
		
		return xpath;
	}

}
