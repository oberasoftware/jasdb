/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package com.oberasoftware.jasdb.rest.client;

import org.junit.Assert;
import org.junit.Test;

/**
 * User: renarj
 * Date: 3/16/12
 * Time: 3:07 PM
 */
public class RestConnectionBuilderTest {
    @Test
    public void testConnectionGeneration() {
        String connectionString = new RestConnectionBuilder().instance("default").bag("bag0").partitions().getConnectionString();
        Assert.assertNotNull(connectionString);
        Assert.assertEquals("Unexpected connection string", "Instance(default)/Bags(bag0)/Partitions", connectionString);
    }

    @Test
    public void testConnectInstanceBags() {
        String connectionString = new RestConnectionBuilder().instance("default").bags().getConnectionString();
        Assert.assertNotNull(connectionString);
        Assert.assertEquals("Unexpected connection string", "Instance(default)/Bags", connectionString);
    }

    @Test
    public void testConnectInstanceBagsPartitionSplitOperation() {
        String connectionString = new RestConnectionBuilder().instance("default").bag("bag0").partition("partitionId").doOperation("split").getConnectionString();
        Assert.assertNotNull(connectionString);
        Assert.assertEquals("Unexpected connection string", "Instance(default)/Bags(bag0)/Partitions(partitionId)/split", connectionString);
    }

}
