package com.oberasoftware.jasdb.rest.model;

import java.util.List;

public class BagCollection implements RestEntity {
	private List<RestBag> bags;
	
	public BagCollection(List<RestBag> bags) {
		this.bags = bags;
	}

    public BagCollection() {

    }

	public List<RestBag> getBags() {
		return bags;
	}

	public void setBags(List<RestBag> bags) {
		this.bags = bags;
	}
}
