package com.oberasoftware.jasdb.console.model;

import java.util.List;

/**
 * @author renarj
 */
public class PageResult {
    private List<WebEntity> entities;
    private int total;
    private int currentPage;
    private int pageSize;

    private String message;

    public boolean firstPage() {
        return currentPage == 0;
    }

    public boolean lastPage() {
        return total % pageSize == currentPage;
    }

    public int getTotal() {
        return total;
    }

    public void setTotal(int total) {
        this.total = total;
    }

    public int getCurrentPage() {
        return currentPage;
    }

    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    public void setCurrentPage(int currentPage) {
        this.currentPage = currentPage;
    }

    public List<WebEntity> getEntities() {
        return entities;
    }

    public void setEntities(List<WebEntity> entities) {
        this.entities = entities;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
