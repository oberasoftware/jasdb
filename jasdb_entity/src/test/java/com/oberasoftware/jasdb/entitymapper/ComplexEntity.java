package com.oberasoftware.jasdb.entitymapper;

import com.oberasoftware.jasdb.api.entitymapper.annotations.EmbeddedEntity;
import com.oberasoftware.jasdb.api.entitymapper.annotations.Id;
import com.oberasoftware.jasdb.api.entitymapper.annotations.JasDBEntity;
import com.oberasoftware.jasdb.api.entitymapper.annotations.JasDBProperty;

import java.util.List;
import java.util.Map;

/**
 * @author Renze de Vries
 */
@JasDBEntity(bagName = "COMPLEX_TEST")
public class ComplexEntity extends BaseEntity {
    private List<String> relatedItems;
    private String name;
    private String customKey;

    private CUSTOM_ENUM customEnum;

    public enum CUSTOM_ENUM {
        VALUE1,
        VALUE2
    }

    private BasicEntity basicEntity;

    private Map<String, String> properties;

    public ComplexEntity(List<String> relatedItems, String emailAddress, CUSTOM_ENUM customEnum,
                         String name, String customKey, Map<String, String> properties) {
        super(emailAddress);
        this.customEnum = customEnum;
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

    @JasDBProperty(nullable = true)
    @EmbeddedEntity
    public BasicEntity getBasicEntity() {
        return basicEntity;
    }

    public void setBasicEntity(BasicEntity basicEntity) {
        this.basicEntity = basicEntity;
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
    public CUSTOM_ENUM getCustomEnum() {
        return customEnum;
    }

    public void setCustomEnum(CUSTOM_ENUM customEnum) {
        this.customEnum = customEnum;
    }

    @JasDBProperty
    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }
}
