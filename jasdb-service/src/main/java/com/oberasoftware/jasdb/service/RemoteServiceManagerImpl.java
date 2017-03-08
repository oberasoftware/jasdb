package com.oberasoftware.jasdb.service;

import nl.renarj.jasdb.api.engine.RemoteServiceManager;
import nl.renarj.jasdb.core.RemoteService;
import nl.renarj.jasdb.core.exceptions.JasDBException;
import nl.renarj.jasdb.core.locator.ServiceInformation;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author renarj
 */
@Component
public class RemoteServiceManagerImpl implements RemoteServiceManager {
    private static final Logger LOG = getLogger(RemoteServiceManagerImpl.class);

    @Autowired(required = false)
    private List<RemoteService> remoteServices;

    @Override
    public void startRemoteServices() throws JasDBException {
        if(remoteServices != null) {
            for (RemoteService remoteService : remoteServices) {
                if (remoteService.isEnabled()) {
                    LOG.info("Starting remote service: {}", remoteService.getClass().getName());
                    remoteService.startService();
                }
            }
        }
    }

    @Override
    public void stopRemoteServices() throws JasDBException {
        if(remoteServices != null) {
            for (RemoteService remoteService : remoteServices) {
                LOG.debug("Stopping remote service endpoint: {}", remoteService.getClass().getName());
                remoteService.stopService();
            }
        }
    }

    @Override
    public List<ServiceInformation> getServiceInformation() {
        if(remoteServices != null) {
            return remoteServices.stream()
                    .map(RemoteService::getServiceInformation)
                    .collect(Collectors.toList());
        } else {
            return new ArrayList<>();
        }
    }
}
