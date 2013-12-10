package nl.renarj.jasdb.rest.loaders;

import nl.renarj.jasdb.rest.exceptions.SyntaxException;
import nl.renarj.jasdb.rest.input.InputElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

public class ModelLoaderFactory {
	private static final Logger LOG = LoggerFactory.getLogger(ModelLoaderFactory.class);
	
	private static final ModelLoaderFactory INSTANCE = new ModelLoaderFactory();
	
	private ServiceLoader<PathModelLoader> serviceLoader = ServiceLoader.load(PathModelLoader.class);
	private Map<String, PathModelLoader> modelLoaders = new HashMap<>();
	
	private ModelLoaderFactory() {
		for(PathModelLoader modelLoader : serviceLoader) {
			for(String modelName : modelLoader.getModelNames()) {
				LOG.info("Loaded element: {} modelloader: {}", modelName, modelLoader);
				modelLoaders.put(modelName, modelLoader);
			}
		}
	}
	
	private PathModelLoader getModelLoader(String element) throws SyntaxException {
		if(modelLoaders.containsKey(element)) {
			return modelLoaders.get(element);
		} else {
			throw new SyntaxException("Unknown input element: " + element);
		}
	}
	
	public static PathModelLoader getModelLoader(InputElement element) throws SyntaxException {
		return INSTANCE.getModelLoader(element.getElementName());
	}
}
