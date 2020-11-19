package com.whycode.mydatacheck.service.impl;

import com.alibaba.fastjson.JSONObject;
import com.whycode.mydatacheck.db.DataBaseFactory;
import com.whycode.mydatacheck.model.CompareLine;
import com.whycode.mydatacheck.model.online.CompareData;
import com.whycode.mydatacheck.service.DataService;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class DataServiceImpl implements DataService {

    private interface COMPARE_RULE {
        String EQUALS = "1";
        String NOT_EQUALS = "2";
        String EMPTY = "3";
    }

    private Map<String, Object> map = new HashMap<>();

    // TODO: 2020/11/6 比较表数据量过大？？？ 只适合数据量小的配置表
    @Override
    public Object execCompare(CompareData compareData) throws Exception {
        HikariDataSource sourceDataSource = DataBaseFactory.getDataSource(compareData.getSourceDataSource());
        NamedParameterJdbcTemplate sourceJdbcTemplate = DataBaseFactory.getJdbcTemplate(sourceDataSource);
        HikariDataSource targetDataSource = DataBaseFactory.getDataSource(compareData.getTargetDataSource());
        NamedParameterJdbcTemplate targetJdbcTemplate = DataBaseFactory.getJdbcTemplate(targetDataSource);

        try {
            for (CompareLine compareLine : compareData.getCompareLineList()) {
                String tableName = compareLine.getTableName();
                String[] keyFields = compareLine.getKeyFieldArray();
                String compareRule = compareLine.getCompareRule();
                String[] compareFields = compareLine.getCompareFieldArray();
                List<Map<String, Object>> compareResultList = new ArrayList<>();
                if (StringUtils.isEmpty(tableName) || keyFields == null || keyFields.length <= 0
                        || StringUtils.isEmpty(compareRule) || compareFields == null || compareFields.length <= 0) {
                    continue;
                }
                String countSql = String.format("Select count(1) as num from %s;", tableName);
                Map<String, Object> countRst = sourceJdbcTemplate.queryForMap(countSql, map);
                if (Integer.parseInt(countRst.get("num").toString()) > 10000) {
                    throw new RuntimeException("表" + tableName + "数据记录超过 10000 条！");
                }
                String sql = String.format("Select * from %s order by %s;", tableName, compareLine.getKeyField());
//                System.out.println(sql);

                List<Map<String, Object>> sourceQueryRst = sourceJdbcTemplate.queryForList(sql, map);

                List<Map<String, Object>> targetQueryRst = targetJdbcTemplate.queryForList(sql, map);

                Map<String, String> keyMap = new HashMap<>();
                if (COMPARE_RULE.EQUALS.equals(compareRule)) {
                    for (Map<String, Object> targetMap : targetQueryRst) {
                        for (String keyField : keyFields) {
                            keyMap.put(keyField, String.valueOf(targetMap.get(keyField)));
                        }
                        JSONObject targetData = JSONObject.parseObject(JSONObject.toJSONString(targetMap));
                        for (Map<String, Object> sourceMap : sourceQueryRst) {
                            if (checkKeyFields(keyMap, sourceMap)) {
                                // 主键一致
                                JSONObject sourceData = JSONObject.parseObject(JSONObject.toJSONString(sourceMap));
                                boolean isAllEquals = true;
                                for (String compareField : compareFields) {
                                    if (!sourceData.getString(compareField).equals(targetData.getString(compareField))) {
                                        isAllEquals = false;
                                        break;
                                    }
                                }
                                // 都相等
                                if (isAllEquals) {
                                    JSONObject jsonObject = new JSONObject();
                                    jsonObject.put("source", sourceMap);
                                    jsonObject.put("target", targetMap);
                                    compareResultList.add(jsonObject);
                                }
                            }
                        }
                    }
                }
                if (COMPARE_RULE.NOT_EQUALS.equals(compareRule)) {
                    for (Map<String, Object> targetMap : targetQueryRst) {
                        JSONObject targetData = JSONObject.parseObject(JSONObject.toJSONString(targetMap));
                        for (String keyField : keyFields) {
                            keyMap.put(keyField, String.valueOf(targetMap.get(keyField)));
                        }
                        for (Map<String, Object> sourceMap : sourceQueryRst) {
                            if (checkKeyFields(keyMap, sourceMap)) {
                                // 主键一致
                                JSONObject sourceData = JSONObject.parseObject(JSONObject.toJSONString(sourceMap));
                                boolean isAllEquals = true;
                                for (String compareField : compareFields) {
                                    if (sourceData.getString(compareField).equals(targetData.getString(compareField))) {
                                        continue;
                                    }
                                    isAllEquals = false;
                                    break;
                                }
                                // 都不相等
                                if (!isAllEquals) {
                                    JSONObject jsonObject = new JSONObject();
                                    jsonObject.put("source", sourceMap);
                                    jsonObject.put("target", targetMap);
                                    compareResultList.add(jsonObject);
                                }
                            }
                        }
                    }
                }
                if (COMPARE_RULE.EMPTY.equals(compareRule)) {
                    for (Map<String, Object> targetMap : targetQueryRst) {
                        JSONObject targetData = JSONObject.parseObject(JSONObject.toJSONString(targetMap));
                        for (String keyField : keyFields) {
                            keyMap.put(keyField, String.valueOf(targetMap.get(keyField)));
                        }
                        for (Map<String, Object> sourceMap : sourceQueryRst) {
                            if (checkKeyFields(keyMap, sourceMap)) {
                                // 主键一致
                                JSONObject sourceData = JSONObject.parseObject(JSONObject.toJSONString(sourceMap));
                                boolean isEmpty = false;
                                for (String compareField : compareFields) {
                                    // 就是源服务器不为空，但目标服务器为空的
                                    if (!StringUtils.isEmpty(sourceData.getString(compareField)) &&
                                            StringUtils.isEmpty(targetData.getString(compareField))) {
                                        isEmpty = true;
                                        break;
                                    }
                                }
                                // 有空数据
                                if (isEmpty) {
                                    JSONObject jsonObject = new JSONObject();
                                    jsonObject.put("source", sourceMap);
                                    jsonObject.put("target", targetMap);
                                    compareResultList.add(jsonObject);
                                }
                            }
                        }
                    }
                }
                compareLine.setCompareResultList(compareResultList);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return null;
        } finally {
            sourceDataSource.close();
            targetDataSource.close();
        }
        return compareData.getCompareLineList();
    }

    @Override
    public void importSourceData(CompareData compareData, File tempFile) throws Exception {
        if (CollectionUtils.isEmpty(compareData.getCompareLineList())) {
            throw new Exception("比较配置行不能为空！");
        }
        HikariDataSource sourceDataSource = DataBaseFactory.getDataSource(compareData.getSourceDataSource());
        NamedParameterJdbcTemplate sourceJdbcTemplate = DataBaseFactory.getJdbcTemplate(sourceDataSource);

        try {
            if (!tempFile.exists()) {
                tempFile.createNewFile();
            }
            for (CompareLine compareLine : compareData.getCompareLineList()) {
                String tableName = compareLine.getTableName();
                String[] keyFields = compareLine.getKeyFieldArray();
                String compareRule = compareLine.getCompareRule();
                String[] compareFields = compareLine.getCompareFieldArray();
                if (StringUtils.isEmpty(tableName) || keyFields == null || keyFields.length <= 0
                        || StringUtils.isEmpty(compareRule) || compareFields == null || compareFields.length <= 0) {
                    continue;
                }
                String countSql = String.format("Select count(1) as num from %s;", tableName);
                Map<String, Object> countRst = sourceJdbcTemplate.queryForMap(countSql, map);
                if (Integer.parseInt(countRst.get("num").toString()) > 10000) {
                    throw new RuntimeException("表" + tableName + "数据记录超过 10000 条！");
                }
                String sql = String.format("Select * from %s order by %s;", tableName, compareLine.getKeyField());
                System.out.println(sql);

                List<Map<String, Object>> sourceQueryRst = sourceJdbcTemplate.queryForList(sql, map);
                compareLine.setCompareResultList(sourceQueryRst);
            }
            FileWriter fw = new FileWriter(tempFile.getAbsoluteFile());
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write(getJSONString(compareData));
            bw.close();
        } catch (Exception e) {
            log.error("查询源数据异常！", e);
        } finally {
            sourceDataSource.close();
        }
    }

    private String getJSONString(CompareData compareData) {
        if (compareData.getCompareLineList() != null && compareData.getCompareLineList().size() > 0) {
            for (CompareLine compareLine : compareData.getCompareLineList()) {
                List<Map<String, Object>> rstList = compareLine.getCompareResultList();
                if (rstList != null && rstList.size() > 0) {
                    for (Map<String, Object> rst : rstList) {
                        rst.forEach((key, value) -> {
                            if (value instanceof Long) {
                                rst.put(key, value.toString());
                            }
                            if (StringUtils.isEmpty(value)) {
                                rst.put(key, "");
                            }
                        });
                    }
                }
            }
        }
        return JSONObject.toJSONString(compareData);
    }

    @Override
    public Object compareWithJsonData(CompareData compareData) {
        HikariDataSource targetDataSource = DataBaseFactory.getDataSource(compareData.getTargetDataSource());
        NamedParameterJdbcTemplate targetJdbcTemplate = DataBaseFactory.getJdbcTemplate(targetDataSource);

        try {
            for (CompareLine compareLine : compareData.getCompareLineList()) {
                String tableName = compareLine.getTableName();
                String[] keyFields = compareLine.getKeyFieldArray();
                String compareRule = compareLine.getCompareRule();
                String[] compareFields = compareLine.getCompareFieldArray();
                List<Map<String, Object>> compareResultList = new ArrayList<>();
                if (StringUtils.isEmpty(tableName) || keyFields == null || keyFields.length <= 0
                        || StringUtils.isEmpty(compareRule) || compareFields == null || compareFields.length <= 0) {
                    continue;
                }
                String sql = String.format("Select * from %s;", tableName);

                List<Map<String, Object>> sourceQueryRst = compareLine.getCompareResultList();

                List<Map<String, Object>> targetQueryRst = targetJdbcTemplate.queryForList(sql, map);

                Map<String, String> keyMap = new HashMap<>();
                if (COMPARE_RULE.EQUALS.equals(compareRule)) {
                    for (Map<String, Object> targetMap : targetQueryRst) {
                        for (String keyField : keyFields) {
                            keyMap.put(keyField, String.valueOf(targetMap.get(keyField)));
                        }
                        JSONObject targetData = JSONObject.parseObject(JSONObject.toJSONString(targetMap));
                        for (Map<String, Object> sourceMap : sourceQueryRst) {
                            if (checkKeyFields(keyMap, sourceMap)) {
                                // 主键一致
                                JSONObject sourceData = JSONObject.parseObject(JSONObject.toJSONString(sourceMap));
                                boolean isAllEquals = true;
                                for (String compareField : compareFields) {
                                    if (!sourceData.getString(compareField).equals(targetData.getString(compareField))) {
                                        isAllEquals = false;
                                        break;
                                    }
                                }
                                // 都相等
                                if (isAllEquals) {
                                    JSONObject jsonObject = new JSONObject();
                                    jsonObject.put("source", sourceMap);
                                    jsonObject.put("target", targetMap);
                                    compareResultList.add(jsonObject);
                                }
                            }
                        }
                    }
                }
                if (COMPARE_RULE.NOT_EQUALS.equals(compareRule)) {
                    for (Map<String, Object> targetMap : targetQueryRst) {
                        JSONObject targetData = JSONObject.parseObject(JSONObject.toJSONString(targetMap));
                        for (String keyField : keyFields) {
                            keyMap.put(keyField, String.valueOf(targetMap.get(keyField)));
                        }
                        for (Map<String, Object> sourceMap : sourceQueryRst) {
                            if (checkKeyFields(keyMap, sourceMap)) {
                                // 主键一致
                                JSONObject sourceData = JSONObject.parseObject(JSONObject.toJSONString(sourceMap));
                                boolean isAllEquals = true;
                                for (String compareField : compareFields) {
                                    if (sourceData.getString(compareField).equals(targetData.getString(compareField))) {
                                        continue;
                                    }
                                    isAllEquals = false;
                                    break;
                                }
                                // 都不相等
                                if (!isAllEquals) {
                                    JSONObject jsonObject = new JSONObject();
                                    jsonObject.put("source", sourceMap);
                                    jsonObject.put("target", targetMap);
                                    compareResultList.add(jsonObject);
                                }
                            }
                        }
                    }
                }
                if (COMPARE_RULE.EMPTY.equals(compareRule)) {
                    for (Map<String, Object> targetMap : targetQueryRst) {
                        JSONObject targetData = JSONObject.parseObject(JSONObject.toJSONString(targetMap));
                        for (String keyField : keyFields) {
                            keyMap.put(keyField, String.valueOf(targetMap.get(keyField)));
                        }
                        for (Map<String, Object> sourceMap : sourceQueryRst) {
                            if (checkKeyFields(keyMap, sourceMap)) {
                                // 主键一致
                                JSONObject sourceData = JSONObject.parseObject(JSONObject.toJSONString(sourceMap));
                                boolean isEmpty = false;
                                for (String compareField : compareFields) {
                                    // 就是源服务器不为空，但目标服务器为空的
                                    if (!StringUtils.isEmpty(sourceData.getString(compareField)) &&
                                            StringUtils.isEmpty(targetData.getString(compareField))) {
                                        isEmpty = true;
                                        break;
                                    }
                                }
                                // 有空数据
                                if (isEmpty) {
                                    JSONObject jsonObject = new JSONObject();
                                    jsonObject.put("source", sourceMap);
                                    jsonObject.put("target", targetMap);
                                    compareResultList.add(jsonObject);
                                }
                            }
                        }
                    }
                }
                compareLine.setCompareResultList(compareResultList);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return null;
        } finally {
            targetDataSource.close();
        }
        return compareData.getCompareLineList();
    }

    private boolean checkKeyFields(Map<String, String> keyMap, Map<String, Object> sourceMap) {
        boolean isOk = true;
        for (Map.Entry<String, String> entry : keyMap.entrySet()) {
            if (!entry.getValue().equals(String.valueOf(sourceMap.get(entry.getKey())))) {
                isOk = false;
                break;
            }
        }
        return isOk;
    }
}
