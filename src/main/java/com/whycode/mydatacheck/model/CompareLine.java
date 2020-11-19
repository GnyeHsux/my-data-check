package com.whycode.mydatacheck.model;

import lombok.Data;
import lombok.ToString;
import org.springframework.util.StringUtils;

import java.util.List;
import java.util.Map;

/**
 * 比较行配置
 */
@ToString
public class CompareLine {

    private String tableName;

    /**
     * 关联字段
     */
    private String keyField;

    /**
     * 比较字段
     */
    private String compareFields;

    /**
     * 比较规则
     */
    private String compareRule;

    private List<Map<String, Object>> compareResultList;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String[] getKeyFieldArray() {
        if (!StringUtils.isEmpty(keyField)) {
            return keyField.split(",");
        }
        return null;
    }

    public void setKeyField(String keyField) {
        this.keyField = keyField;
    }

    public String[] getCompareFieldArray() {
        if (!StringUtils.isEmpty(compareFields)) {
            return compareFields.split(",");
        }
        return null;
    }

    public void setCompareFields(String compareFields) {
        this.compareFields = compareFields;
    }

    public String getCompareRule() {
        return compareRule;
    }

    public void setCompareRule(String compareRule) {
        this.compareRule = compareRule;
    }

    public List<Map<String, Object>> getCompareResultList() {
        return compareResultList;
    }

    public void setCompareResultList(List<Map<String, Object>> compareResultList) {
        this.compareResultList = compareResultList;
    }

    public String getKeyField() {
        return keyField;
    }

    public String getCompareFields() {
        return compareFields;
    }
}
