package nl.renarj.jasdb.service.wrappers;

import nl.renarj.core.exceptions.ReflectionException;
import nl.renarj.core.utilities.ReflectionLoader;
import nl.renarj.core.utilities.configuration.Configuration;
import nl.renarj.jasdb.api.kernel.KernelContext;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.service.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Renze de Vries
 */
public class ServiceWrapperFactory {
    private static final Logger LOG = LoggerFactory.getLogger(ServiceWrapperFactory.class);

    private List<String> wrapperClasses = new ArrayList<String>();
    private KernelContext kernelContext;

    public ServiceWrapperFactory(KernelContext kernelContext) {
        this.kernelContext = kernelContext;
        Configuration configuration = kernelContext.getConfiguration();
        List<Configuration> wrapperConfigurations = configuration.getChildConfigurations("/jasdb/modules/module[@type='storageservice']/wrappers/wrapper");
        for(Configuration wrapperConfiguration : wrapperConfigurations) {
            if(wrapperConfiguration.hasAttribute("class")) {
                String wrapperClass = wrapperConfiguration.getAttribute("class");
                LOG.info("Installing service wrapper: {}", wrapperClass);
                wrapperClasses.add(wrapperClass);
            } else {
                LOG.warn("Skipping wrapper: {} no class attribute present", wrapperConfiguration);
            }
        }
    }

    public StorageService wrap(StorageService storageService) throws JasDBStorageException {
        LOG.debug("Running: {} wrappers for storage service: {}", wrapperClasses.size(), storageService);
        StorageService currentService = storageService;
        for(String wrapperClass : wrapperClasses) {
            LOG.debug("Activating service wrapper: {}", wrapperClass);
            try {
                ServiceWrapper serviceWrapper = ReflectionLoader.loadClass(ServiceWrapper.class, wrapperClass, new Object[]{});
                serviceWrapper.wrap(kernelContext, currentService);

                currentService = serviceWrapper;
            } catch(ReflectionException e) {
                throw new JasDBStorageException("Unable to load service wrapper: " + wrapperClass, e);
            }
        }
        return currentService;
    }
}
