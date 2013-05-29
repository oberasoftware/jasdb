package nl.renarj.jasdb.api.metadata;

import nl.renarj.jasdb.core.exceptions.MetadataParseException;

public class IndexDefinition {
    private int indexType;
    private String indexName;
    private String headerDescriptor;
    private String valueDescriptor;
    
    public IndexDefinition(String indexName, String headerDescriptor, String valueDescriptor, int indexType) {
        this.headerDescriptor = headerDescriptor;
        this.valueDescriptor = valueDescriptor;
        this.indexType = indexType;
        this.indexName = indexName;
    }
    
    public String getIndexName() {
        return indexName;
    }
    
    public int getIndexType() {
        return indexType;
    }

    public String getHeaderDescriptor() {
        return headerDescriptor;
    }

    public String getValueDescriptor() {
        return valueDescriptor;
    }
    
    public String toHeader() {
        StringBuilder builder = new StringBuilder();
        
        builder.append(indexName).append("/");
        builder.append(headerDescriptor).append("/");
        builder.append(valueDescriptor).append("/");
        builder.append(indexType).append("/");
        
        return builder.toString();
    }
    
    public static IndexDefinition fromHeader(String header) throws MetadataParseException {
        String[] definitionSections = header.split("/");
        if(definitionSections.length == 4) {
            try {
                String indexName = definitionSections[0];
                String headerDescriptor = definitionSections[1];
                String valueDescriptor = definitionSections[2];
                int indexType = Integer.parseInt(definitionSections[3]);

                return new IndexDefinition(indexName, headerDescriptor, valueDescriptor, indexType);
            } catch(NumberFormatException e) {
                throw new MetadataParseException("Unable to get index type for heade: " + header);
            }
        } else {
            throw new MetadataParseException("Unable to parse index metadata section");
        }
    }

    @Override
    public int hashCode() {
        return toHeader().hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if(o instanceof IndexDefinition) {
            IndexDefinition other = (IndexDefinition) o;
            if(other.getHeaderDescriptor().equals(headerDescriptor)
                    && other.getValueDescriptor().equals(valueDescriptor)
                    && other.getIndexName().equals(indexName)
                    && other.getIndexType() == indexType) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return toHeader();
    }
}
