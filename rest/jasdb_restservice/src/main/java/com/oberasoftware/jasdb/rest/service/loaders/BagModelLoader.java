package com.oberasoftware.jasdb.rest.service.loaders;

import com.oberasoftware.jasdb.api.engine.DBInstanceFactory;
import com.oberasoftware.jasdb.api.exceptions.JasDBException;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.exceptions.RestException;
import com.oberasoftware.jasdb.api.model.Bag;
import com.oberasoftware.jasdb.api.session.DBInstance;
import com.oberasoftware.jasdb.core.utils.StringUtils;
import com.oberasoftware.jasdb.engine.StorageService;
import com.oberasoftware.jasdb.engine.StorageServiceFactory;
import com.oberasoftware.jasdb.rest.model.BagCollection;
import com.oberasoftware.jasdb.rest.model.ErrorEntity;
import com.oberasoftware.jasdb.rest.model.RestBag;
import com.oberasoftware.jasdb.rest.model.RestEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

import static com.oberasoftware.jasdb.rest.service.loaders.DataUtil.*;
import static org.springframework.web.bind.annotation.RequestMethod.*;

@RestController
public class BagModelLoader {
    private static final Logger LOG = LoggerFactory.getLogger(BagModelLoader.class);

    private final DBInstanceFactory instanceFactory;
    private final StorageServiceFactory storageServiceFactory;

    @Autowired
    public BagModelLoader(DBInstanceFactory instanceFactory, StorageServiceFactory storageServiceFactory) {
        this.instanceFactory = instanceFactory;
        this.storageServiceFactory = storageServiceFactory;
    }

    @RequestMapping(value = "/Instances({instanceId})/Bags", produces = "application/json", method = GET)
    public ResponseEntity<RestEntity> loadBags(@PathVariable String instanceId) throws JasDBException {
        return handleList(instanceId);
    }

    @RequestMapping(value = "/Bags", produces = "application/json", method = GET)
    public ResponseEntity<RestEntity> loadBags() throws JasDBException {
        return handleList(null);
    }

    @RequestMapping(value = "/Bags({bagName})", produces = "application/json", method = GET)
    public ResponseEntity<RestEntity> loadBag(@PathVariable String bagName) throws JasDBException {
        return doSearch(null, bagName);
    }

    @RequestMapping(value = "/Instances({instanceId})/Bags({bagName})", produces = "application/json", method = GET)
    public ResponseEntity<RestEntity> loadBag(@PathVariable String instanceId, @PathVariable String bagName) throws JasDBException {
        return doSearch(instanceId, bagName);
    }

    @RequestMapping(value = "/Bags", consumes = "application/json", produces = "application/json", method = POST)
    public ResponseEntity<RestEntity> writeEntry(@RequestBody RestBag bag) throws JasDBException {
        return createBag(null, bag);
    }

    @RequestMapping(value = "/Instances({instanceId})/Bags", consumes = "application/json", produces = "application/json", method = POST)
	public ResponseEntity<RestEntity> writeEntry(@PathVariable String instanceId, @RequestBody RestBag bag) throws JasDBException {
        return createBag(instanceId, bag);
	}

    @RequestMapping(value = "/Instances({instanceId})/Bags({bagName})", method = DELETE)
    public ResponseEntity<?> removeEntry(@PathVariable String instanceId, @PathVariable String bagName) throws JasDBException {
        return removeBag(instanceId, bagName);
    }

    @RequestMapping(value = "/Bags({bagName})", method = DELETE)
    public ResponseEntity<?> removeEntry(@PathVariable String bagName) throws JasDBException {
        return removeBag(null, bagName);
    }

    @RequestMapping(value = "/Instances({instanceId})/Bags({bagName})/flush")
    public ResponseEntity<RestEntity> doOperation(@PathVariable String instanceId, @PathVariable String bagName) throws JasDBException {
        StorageService storageService = storageServiceFactory.getOrCreateStorageService(instanceId, bagName);
        storageService.flush();

        return loadBag(instanceId, bagName);
    }

    private ResponseEntity<?> removeBag(String instanceId, String bagName) throws JasDBException {
        DBInstance instance = getInstance(instanceFactory, instanceId);

        if(storageServiceFactory.getStorageService(instance.getInstanceId(), bagName) != null) {
            storageServiceFactory.removeStorageService(instance.getInstanceId(), bagName);
            return new ResponseEntity<>(HttpStatus.NO_CONTENT);
        } else {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
    }

    private ResponseEntity<RestEntity> createBag(String instanceId, RestBag bag) throws JasDBException {
        DBInstance instance = getInstance(instanceFactory, instanceId);

        if(StringUtils.stringNotEmpty(bag.getName())) {
            StorageService storageService = storageServiceFactory.getOrCreateStorageService(instance.getInstanceId(), bag.getName());

            return ok(new RestBag(instance.getInstanceId(), bag.getName(), storageService.getSize(), storageService.getDiskSize()));
        } else {
            throw new RestException("Cannot create bag, no name specified");
        }
    }

    private ResponseEntity<RestEntity> doSearch(String instanceId, String bagName) throws JasDBException {
        DBInstance instance = getInstance(instanceFactory, instanceId);

        try {
            StorageService storageService = storageServiceFactory.getStorageService(instance.getInstanceId(), bagName);
            if(storageService != null) {
                LOG.debug("Found a bag with name: {}", bagName);
                return ok(new RestBag(instance.getInstanceId(), bagName, storageService.getSize(), storageService.getDiskSize()));
            } else {
                return notFound(new ErrorEntity(404, "No bag was found with name: " + bagName));
            }
        } catch(JasDBStorageException e) {
            throw new RestException("Unable to load bag metadata", e);
        }
	}
	
	private ResponseEntity<RestEntity> handleList(String instanceId) throws RestException {
		LOG.debug("Retrieving full list of bags on storage instance: {}", instanceId);
		List<RestBag> bags = new ArrayList<>();
		try {
		    DBInstance instance = getInstance(instanceFactory, instanceId);
			for(Bag bag : instance.getBags()) {
                StorageService storageService = storageServiceFactory.getStorageService(instance.getInstanceId(), bag.getName());
				bags.add(new RestBag(instance.getInstanceId(), bag.getName(), storageService.getSize(), storageService.getDiskSize()));
			}
		} catch(JasDBStorageException e) {
			throw new RestException("Unable to load bags", e);
		}
		
		return ok(new BagCollection(bags));
	}

}
