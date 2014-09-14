package nl.renarj.core.utilities.configuration;

import nl.renarj.core.exceptions.CoreConfigException;
import nl.renarj.core.utilities.XMLReader;
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

public class Configuration {
	private static final Logger log = LoggerFactory.getLogger(Configuration.class);
	
	private Node configurationNode;
	
	protected Configuration(Node configurationNode) {
		this.configurationNode = configurationNode;
	}
	
	public String getAttribute(String attributeName) {
		return getAttribute(attributeName, null);
	}
	
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
			log.error("Unable to find the value for attribute: " + attributeName, e.getMessage());
			return defaultValue;
		}
	}
	
	public int getAttribute(String attributeName, int defaultValue) {
		String value = getAttribute(attributeName, null);
		if(value != null) {
			try {
				return Integer.parseInt(value);
			} catch(NumberFormatException e) {
				log.error("Unable to parse configuration attribute: {} with value: {}", attributeName, value);
			}
		}
		
		return defaultValue;
	}
	
	public boolean getAttribute(String attributeName, boolean defaultValue) {
		String value = getAttribute(attributeName, null);
		if(value != null) {
			return Boolean.valueOf(value) ? true : defaultValue;
		} else {
			return defaultValue;
		}
	}
	
	public boolean hasAttribute(String attributeName) {
		return getAttribute(attributeName) != null ? true : false;
	}
	
	public String getName() {
		return this.configurationNode.getNodeName();
	}
	
	public Configuration getChildConfiguration(String configurationPath) {
		try {
			XPath xpath = getXpath();
			
			Node foundNode = (Node) xpath.evaluate(configurationPath, this.configurationNode, XPathConstants.NODE);
			if(foundNode != null) {
				return new Configuration(foundNode);
			}
		} catch(XPathExpressionException e) {
			log.error("Unable to find the configuration for path: " + configurationPath, e.getMessage());
		}
		
		return null;
	}
	
	public List<Configuration> getChildConfigurations(String configurationPath) {
		try {
			XPath xpath = getXpath();
			NodeList foundNodes = (NodeList) xpath.evaluate(configurationPath, this.configurationNode, XPathConstants.NODESET);
			
			return convertNodeList(foundNodes);
		} catch(XPathExpressionException e) {
			log.error("Unable to find the child configurations for path: " + configurationPath, e.getMessage());
		}
		
		return Collections.emptyList();
	}
	
	public List<Configuration> getChildren() {
		return convertNodeList(configurationNode.getChildNodes());
	}
	
	private List<Configuration> convertNodeList(NodeList nodeList) {
		List<Configuration> loadedConfigurations = new ArrayList<Configuration>();
		for(int i=0; i<nodeList.getLength(); i++) {
			if(nodeList.item(i) instanceof Element) {
				loadedConfigurations.add(new Configuration(nodeList.item(i)));
			}
		}

		return loadedConfigurations;
	}
	
	private XPath getXpath() {
		XPathFactory xpathFactory = XPathFactory.newInstance();
		XPath xpath = xpathFactory.newXPath();
		
		return xpath;
	}
	
	public static Configuration loadConfiguration(String fileName) throws CoreConfigException {
		log.info("Loading configuration: {}", fileName);
		Document document = XMLReader.getDocument(fileName);
		
		return new Configuration(document.getDocumentElement());
	}
}
