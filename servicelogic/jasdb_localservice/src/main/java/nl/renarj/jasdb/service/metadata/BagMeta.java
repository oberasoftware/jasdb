/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.service.metadata;

import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.api.metadata.Bag;
import nl.renarj.jasdb.api.metadata.IndexDefinition;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;

import java.util.ArrayList;
import java.util.List;

/**
 * User: renarj
 * Date: 2/5/12
 * Time: 11:13 PM
 */
public class BagMeta implements Bag {

    private String name;
    private String instanceId;
    private List<IndexDefinition> indexDefinitions;
    
    public BagMeta(String instanceId, String name, List<IndexDefinition> indexDefinitions) {
        this.instanceId = instanceId;
        this.name = name;
        this.indexDefinitions = indexDefinitions;
    }

    public static BagMeta fromEntity(SimpleEntity entity) throws JasDBStorageException {
        String instance = entity.getValue(Constants.INSTANCE).toString();
        String name = entity.getValue(Constants.NAME).toString();

        List<IndexDefinition> indexDefinitionList = new ArrayList<IndexDefinition>();
        if(entity.hasProperty(Constants.INDEXES)) {
            for(Object indexDefinition : entity.getValues(Constants.INDEXES)) {
                indexDefinitionList.add(IndexDefinition.fromHeader(indexDefinition.toString()));
            }
        }
        return new BagMeta(instance, name, indexDefinitionList);
    }

    public static SimpleEntity toEntity(Bag bag) throws JasDBStorageException {
        SimpleEntity entity = new SimpleEntity();
        entity.addProperty(Constants.META_TYPE, Constants.BAG_TYPE);
        entity.addProperty(Constants.INSTANCE, bag.getInstanceId());
        entity.addProperty(Constants.NAME, bag.getName());

        for(IndexDefinition indexDefinition : bag.getIndexDefinitions()) {
            entity.addProperty(Constants.INDEXES, indexDefinition.toHeader());
        }
        return entity;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getInstanceId() {
        return instanceId;
    }

    @Override
    public List<IndexDefinition> getIndexDefinitions() {
        return indexDefinitions;
    }

    @Override
    public boolean equals(Object o) {
        if(o != null && o instanceof BagMeta) {
            BagMeta bagMeta = (BagMeta) o;
            return bagMeta.instanceId.equals(instanceId) && bagMeta.name.equals(name);
        }

        return false;
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + instanceId.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "BagMeta{" +
                "name='" + name + '\'' +
                ", instanceId='" + instanceId + '\'' +
                '}';
    }
}
