package nl.renarj.jasdb.rest.loaders;

import nl.renarj.jasdb.rest.exceptions.SyntaxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class ModelLoaderFactory {
	private static final Logger LOG = LoggerFactory.getLogger(ModelLoaderFactory.class);
	
	private Map<String, PathModelLoader> modelLoaderMap = new HashMap<>();

    @Autowired
    private List<PathModelLoader> modelLoaders;

    @PostConstruct
	public void init() {
		for(PathModelLoader modelLoader : modelLoaders) {
			for(String modelName : modelLoader.getModelNames()) {
				LOG.info("Loaded element: {} modelloader: {}", modelName, modelLoader);
                modelLoaderMap.put(modelName, modelLoader);
			}
		}
	}
	
	public PathModelLoader getModelLoader(String element) throws SyntaxException {
		if(modelLoaderMap.containsKey(element)) {
			return modelLoaderMap.get(element);
		} else {
			throw new SyntaxException("Unknown input element: " + element);
		}
	}
}
