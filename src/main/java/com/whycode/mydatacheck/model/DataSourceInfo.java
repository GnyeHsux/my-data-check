package com.whycode.mydatacheck.model;

import lombok.Data;

@Data
public class DataSourceInfo {

    private String host;

    private String port;

    private String database;

    private String username;

    private String password;

    public String getUrl() {
        return String.format("jdbc:mysql://%s:%s/%s?useUnicode=true&characterEncoding=utf8&autoReconnect=true&allowMultiQueries=true", getHost(), getPort(), getDatabase());
    }
}
