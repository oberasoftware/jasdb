package com.oberasoftware.jasdb.entitymapper;

import com.oberasoftware.jasdb.api.entitymapper.annotations.Id;
import com.oberasoftware.jasdb.api.entitymapper.annotations.JasDBEntity;
import com.oberasoftware.jasdb.api.entitymapper.annotations.JasDBProperty;

import java.util.List;
import java.util.Map;

/**
 * @author Renze de Vries
 */
@JasDBEntity(bagName = "COMPLEX_TEST")
public class ComplexEntity {
    private List<String> relatedItems;
    private String name;
    private String customKey;

    private Map<String, String> properties;

    public ComplexEntity(List<String> relatedItems, String name, String customKey, Map<String, String> properties) {
        this.relatedItems = relatedItems;
        this.name = name;
        this.customKey = customKey;
        this.properties = properties;
    }

    public ComplexEntity() {
    }

    @JasDBProperty(name = "ITEMS")
    public List<String> getRelatedItems() {
        return relatedItems;
    }

    public void setRelatedItems(List<String> relatedItems) {
        this.relatedItems = relatedItems;
    }

    public String getName() {
        return name;
    }

    @JasDBProperty
    public void setName(String name) {
        this.name = name;
    }

    @JasDBProperty
    @Id
    public String getCustomKey() {
        return customKey;
    }

    public void setCustomKey(String customKey) {
        this.customKey = customKey;
    }

    @JasDBProperty
    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }
}
