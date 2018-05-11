package com.oberasoftware.jasdb.rest.model;

import java.util.List;

/**
 * @author Renze de Vries
 * Date: 3-6-12
 * Time: 16:43
 */
public class IndexCollection implements RestEntity {
    private List<IndexEntry> indexEntryList;

    public IndexCollection() {

    }

    public IndexCollection(List<IndexEntry> indexEntryList) {
        this.indexEntryList = indexEntryList;
    }

    public List<IndexEntry> getIndexEntryList() {
        return indexEntryList;
    }

    public void setIndexEntryList(List<IndexEntry> indexEntryList) {
        this.indexEntryList = indexEntryList;
    }
}
