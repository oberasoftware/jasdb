/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package com.oberasoftware.jasdb.rest.client;

/**
* User: renarj
* Date: 3/16/12
* Time: 3:06 PM
*/
public class RestConnectionBuilder {
    private StringBuilder builder;

    public RestConnectionBuilder() {
        builder = new StringBuilder();
    }

    public RestConnectionBuilder instance(String instance) {
        return appendRequest("Instance", instance);
    }

    public RestConnectionBuilder instance() {
        return appendRequest("Instance", null);
    }

    public RestConnectionBuilder bags() {
        return bag(null);
    }

    public RestConnectionBuilder bag(String bag) {
        return appendRequest("Bags", bag);
    }

    public RestConnectionBuilder partitions() {
        return partition(null);
    }
    
    public RestConnectionBuilder partition(String partitionId) {
        return appendRequest("Partitions", partitionId);
    }
    
    public RestConnectionBuilder entities() {
        return entityById(null);
    }
    
    public RestConnectionBuilder entityById(String entityId) {
        return appendRequest("Entities", entityId);
    }

    public RestConnectionBuilder entities(String query) {
        return appendRequest("Entities", query);
    }

    public RestConnectionBuilder indexes() {
        return appendRequest("Indexes", null);
    }

    public RestConnectionBuilder index(String indexName) {
        return appendRequest("Indexes", indexName);
    }
    
    public RestConnectionBuilder doOperation(String operation) {
        return appendRequest(operation, null);
    }

    public String getConnectionString() {
        return builder.toString();
    }

    private RestConnectionBuilder appendRequest(String entity, String value) {
        checkAppend();
        builder.append(entity);
        if(value != null) {
           builder.append("(").append(value).append(")");
        }

        return this;
    }

    private void checkAppend() {
        if(builder.length() > 0) {
            builder.append("/");
        }
    }
}
