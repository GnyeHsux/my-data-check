package com.whycode.mydatacheck.model.online;

import com.whycode.mydatacheck.model.CompareLine;
import com.whycode.mydatacheck.model.DataSourceInfo;
import lombok.Data;
import lombok.ToString;

import java.util.List;

/**
 * 线上数据比较入参
 */
@Data
@ToString
public class CompareData {

    /**
     * 源数据源
     */
    private DataSourceInfo sourceDataSource;

    /**
     * 比较数据源
     */
    private DataSourceInfo targetDataSource;

    /**
     * 比较配置行
     */
    private List<CompareLine> compareLineList;

}
