package nl.renarj.core.utilities;

import nl.renarj.core.exceptions.ConfigurationNotFoundException;
import nl.renarj.core.exceptions.CoreConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

public class XMLReader {
	private static final Logger LOG = LoggerFactory.getLogger(XMLReader.class);
	
	public static Document getDocument(String configFile) throws CoreConfigException {
		URL resourceUrl = Thread.currentThread().getContextClassLoader().getResource(configFile);
        
		if(resourceUrl != null) {
            LOG.debug("Reading XML file: " + resourceUrl.toString());
            return parseDocument(resourceUrl);
		} else {
			throw new ConfigurationNotFoundException("Unable to load configuration, could not be found on classpath");
		}
	}

	private static Document parseDocument(URL resourceUrl) throws CoreConfigException {
		try {
            InputStream inputSource = resourceUrl.openStream();
            DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();

            return documentBuilder.parse(inputSource);
		} catch(ParserConfigurationException e) {
			throw new CoreConfigException("No XML Parser was provided", e);
		} catch (SAXException e) {
			throw new CoreConfigException("Unable to parse the XML document", e);
		} catch (IOException e) {
			throw new CoreConfigException("Unable to read input source document", e);
		}
	}
}
