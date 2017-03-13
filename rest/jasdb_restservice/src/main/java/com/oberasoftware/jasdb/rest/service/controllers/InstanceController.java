package com.oberasoftware.jasdb.rest.service.controllers;

import com.oberasoftware.jasdb.api.engine.DBInstanceFactory;
import com.oberasoftware.jasdb.api.engine.EngineManager;
import com.oberasoftware.jasdb.api.exceptions.ConfigurationException;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.exceptions.RestException;
import com.oberasoftware.jasdb.api.session.DBInstance;
import com.oberasoftware.jasdb.rest.model.InstanceCollection;
import com.oberasoftware.jasdb.rest.model.InstanceRest;
import com.oberasoftware.jasdb.rest.model.RestEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

import static org.springframework.web.bind.annotation.RequestMethod.*;

@RestController
public class InstanceController {
    private static final Logger LOG = LoggerFactory.getLogger(InstanceController.class);

    private final DBInstanceFactory instanceFactory;

    private final EngineManager engineManager;

    @Autowired
    public InstanceController(DBInstanceFactory instanceFactory, EngineManager engineManager) {
        this.instanceFactory = instanceFactory;
        this.engineManager = engineManager;
    }

    @RequestMapping(value = "/Instances", produces = "application/json", method = GET)
    public RestEntity getInstances() throws RestException {
        return new InstanceCollection(loadInstances());
    }

    @RequestMapping(value = "/Instances({instanceId})", produces = "application/json", method = GET)
    public RestEntity getInstance(@PathVariable String instanceId) throws RestException {
        return loadInstance(instanceId);
    }

    private List<InstanceRest> loadInstances() throws RestException {
        List<InstanceRest> instances = new ArrayList<>();
        for(DBInstance instance : instanceFactory.listInstances()) {
            instances.add(new InstanceRest(instance.getPath(), "OK", engineManager.getEngineVersion(), instance.getInstanceId()));
        }
        return instances;
    }
    
    private InstanceRest loadInstance(String instanceId) throws RestException {
        try {
            DBInstance dbInstance = instanceFactory.getInstance(instanceId);

            return new InstanceRest(dbInstance.getPath(), "OK", engineManager.getEngineVersion(), dbInstance.getInstanceId());
        } catch(ConfigurationException e) {
            throw new RestException("Unable to retrieve the instance", e);
        }
    }

	@RequestMapping(value = "/Instances", method = POST, consumes = "application/json", produces = "application/json")
	public RestEntity writeEntry(@RequestBody InstanceRest dbInstance) throws RestException {
        try {
            instanceFactory.addInstance(dbInstance.getInstanceId());

            return getInstance(dbInstance.getInstanceId());
        } catch(JasDBStorageException e) {
            throw new RestException("Unable to create new instance: " + e.getMessage());
        }
	}

    @RequestMapping(value = "/Instances({instanceId})", method = DELETE, produces = "application/json")
    public RestEntity removeEntry(@PathVariable String instanceId) throws RestException {
        try {
            LOG.debug("Receiving a instance delete operation for instance: {}", instanceId);
            instanceFactory.deleteInstance(instanceId);

            return null;
        } catch(JasDBStorageException e) {
            throw new RestException("Unable to remove instance: " + instanceId);
        }
    }
}
