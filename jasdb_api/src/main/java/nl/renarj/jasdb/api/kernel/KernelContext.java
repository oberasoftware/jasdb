package nl.renarj.jasdb.api.kernel;

import com.google.inject.Injector;
import nl.renarj.core.utilities.configuration.Configuration;
import nl.renarj.jasdb.api.metadata.MetadataStore;
import nl.renarj.jasdb.core.locator.NodeInformation;

/**
 * @author Renze de Vries
 */
public class KernelContext {
    private Injector injector;
    private Configuration configuration;
    private NodeInformation nodeInformation;
    private MetadataStore metadataStore;

    public KernelContext(Injector injector, Configuration configuration, NodeInformation nodeInformation, MetadataStore metadataStore) {
        this.injector = injector;
        this.configuration = configuration;
        this.nodeInformation = nodeInformation;
        this.metadataStore = metadataStore;
    }

    public MetadataStore getMetadataStore() {
        return metadataStore;
    }

    public NodeInformation getNodeInformation() {
        return nodeInformation;
    }

    public Injector getInjector() {
        return injector;
    }

    public Configuration getConfiguration() {
        return configuration;
    }
}
