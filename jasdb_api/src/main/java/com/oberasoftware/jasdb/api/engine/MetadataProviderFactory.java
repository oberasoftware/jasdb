package com.oberasoftware.jasdb.api.engine;

public interface MetadataProviderFactory {

    /**
     * Gets a metadata provider that can handle non standard metadata stored in the metadata store
     * @param type The type of metadata provider requested
     * @param <T> The subtype of the metadata provider
     * @return The metadataprovider if it exists
     */
    <T extends MetadataProvider> T getProvider(String type);
}
