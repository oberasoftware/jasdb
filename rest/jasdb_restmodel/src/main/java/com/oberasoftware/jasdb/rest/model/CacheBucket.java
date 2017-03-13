package com.oberasoftware.jasdb.rest.model;

public class CacheBucket implements RestEntity {
	private String name;
    private int size;
    private long memSize;
	
	public CacheBucket(String name, int size, long memSize) {
		this.name = name;
        this.size = size;
        this.memSize = memSize;
	}
	
	public String getName() {
		return this.name;
	}
	
	public int getSize() {
		return this.size;
	}
	
	public long getMemSize() {
		return this.memSize;
	}
}
