package com.oberasoftware.jasdb.rest.model;

public class RestBag implements RestEntity {
    private String instanceId;
	private String name;
	private long size;
	private long diskSize;
	
	public RestBag(String instanceId, String name, long size, long diskSize) {
        this.instanceId = instanceId;
		this.name = name;
		this.size = size;
		this.diskSize = diskSize;
	}

    public RestBag() {

    }
    
    public String getInstanceId() {
        return this.instanceId;
    }

	public String getName() {
		return name;
	}

	public long getSize() {
		return size;
	}

	public long getDiskSize() {
		return diskSize;
	}
}
