package com.oberasoftware.jasdb.rest.model;


import com.oberasoftware.jasdb.api.model.Instance;

public class InstanceRest implements RestEntity, Instance {
	private String path;
	private String version;
	private String instanceId;
	private String status;
	
	public InstanceRest(String path, String status, String version, String instanceId) {
		this.path = path;
		this.version = version;
		this.instanceId = instanceId;
        this.status = status;
	}

    public InstanceRest() {

    }
	
	public String getStatus() {
		return this.status;
	}

	public String getVersion() {
		return this.version;
	}

    @Override
	public String getInstanceId() {
		return this.instanceId;
	}

    @Override
	public String getPath() {
		return this.path;
	}
}
