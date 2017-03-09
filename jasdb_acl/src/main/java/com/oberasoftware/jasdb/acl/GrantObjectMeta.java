package com.oberasoftware.jasdb.acl;

import com.oberasoftware.jasdb.engine.metadata.Constants;
import nl.renarj.jasdb.api.EmbeddedEntity;
import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.api.acl.AccessMode;
import nl.renarj.jasdb.api.metadata.Grant;
import nl.renarj.jasdb.api.metadata.GrantObject;
import nl.renarj.jasdb.api.properties.EntityValue;
import nl.renarj.jasdb.api.properties.Property;
import nl.renarj.jasdb.api.properties.Value;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Renze de Vries
 */
public class GrantObjectMeta implements GrantObject {

    private String objectName;
    private Map<String, Grant> userGrants;

    public GrantObjectMeta(String objectName, Map<String, Grant> userGrants) {
        this.objectName = objectName;
        this.userGrants = userGrants;
    }

    public GrantObjectMeta(String objectName, Grant... grants) {
        this.objectName = objectName;
        this.userGrants = new ConcurrentHashMap<>();
        for(Grant grant : grants) {
            userGrants.put(grant.getGrantedUsername(), grant);
        }
    }

    public static GrantObject fromEntity(SimpleEntity entity) {
        String grantObject = entity.getValue(Constants.GRANT_OBJECT).toString();
        Map<String, Grant> userGrants = new ConcurrentHashMap<>();
        Property grantsProperty = entity.getProperty(Constants.GRANTS);
        for(Value grantValue : grantsProperty.getValues()) {
            EntityValue entityValue = (EntityValue) grantValue;
            String grantUser = entityValue.toEntity().getValue(Constants.GRANT_USER).toString();
            String grantMode = entityValue.toEntity().getValue(Constants.GRANT_MODE).toString();
            userGrants.put(grantUser, new GrantMeta(grantUser, AccessMode.fromMode(grantMode)));
        }
        return new GrantObjectMeta(grantObject, userGrants);
    }

    public static SimpleEntity toEntity(GrantObject grantObject) {
        SimpleEntity entity = new SimpleEntity();
        entity.addProperty(Constants.GRANT_OBJECT, grantObject.getObjectName());
        for(Grant grant : grantObject.getGrants()) {
            EmbeddedEntity grantEntity = new EmbeddedEntity();
            grantEntity.setProperty(Constants.GRANT_USER, grant.getGrantedUsername());
            grantEntity.setProperty(Constants.GRANT_MODE, grant.getAccessMode().getMode());
            entity.addEntity(Constants.GRANTS, grantEntity);
        }
        return entity;
    }


    @Override
    public String getObjectName() {
        return objectName;
    }

    @Override
    public void addGrant(Grant grant) {
        userGrants.put(grant.getGrantedUsername(), grant);
    }

    @Override
    public boolean isGranted(String userName, AccessMode mode) {
        Grant grant = getGrant(userName);

        if(grant != null) {
            //check the the grants for the user has a higher rank than the desired access rank
            return grant.getAccessMode().getRank() >= mode.getRank();
        } else {
            return false;
        }
    }

    @Override
    public Grant getGrant(String userName) {
        return userGrants.get(userName);
    }

    @Override
    public List<Grant> getGrants() {
        return new ArrayList<>(userGrants.values());
    }

    @Override
    public void removeGrant(String userName) {
        userGrants.remove(userName);
    }

    @Override
    public String toString() {
        return "GrantObjectMeta{" +
                "objectName='" + objectName + '\'' +
                ", userGrants=" + userGrants +
                '}';
    }
}
