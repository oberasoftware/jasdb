package com.oberasoftware.jasdb.rest.model;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Renze de Vries
 */
public class RestUserList implements RestEntity {
    private List<RestUser> users;

    public RestUserList() {

    }

    public RestUserList(List<String> userList) {
        users = new ArrayList<>(userList.size());
        for(String user: userList) {
            users.add(new RestUser(user, null, null));
        }
    }

    public List<RestUser> getUsers() {
        return users;
    }

    public void setUsers(List<RestUser> users) {
        this.users = users;
    }
}
