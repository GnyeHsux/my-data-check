package com.whycode.mydatacheck.db;

import com.whycode.mydatacheck.model.DataSourceInfo;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.util.concurrent.ConcurrentHashMap;

public class DataBaseFactory {

    private static final ConcurrentHashMap<String, NamedParameterJdbcTemplate> DATA_SOURCE_MAP = new ConcurrentHashMap<>();

    public static HikariDataSource getDataSource(DataSourceInfo dataSourceInfo) {

        final HikariDataSource dataSource = new HikariDataSource();

        dataSource.setJdbcUrl(dataSourceInfo.getUrl());
        dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
        dataSource.setUsername(dataSourceInfo.getUsername());
        dataSource.setPassword(dataSourceInfo.getPassword());

        dataSource.setMaximumPoolSize(2);
        dataSource.setMinimumIdle(1);
        dataSource.setAutoCommit(true);
        dataSource.addDataSourceProperty("cachePrepStmts", "true");
        dataSource.addDataSourceProperty("prepStmtCacheSize", "250");
        dataSource.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");

        return dataSource;
    }

    public static NamedParameterJdbcTemplate getJdbcTemplate(HikariDataSource dataSource) {
        return new NamedParameterJdbcTemplate(dataSource);
    }
}
