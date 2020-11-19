package com.whycode.mydatacheck.controller;

import com.alibaba.fastjson.JSONObject;
import com.whycode.mydatacheck.model.Response;
import com.whycode.mydatacheck.model.online.CompareData;
import com.whycode.mydatacheck.service.DataService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;
import java.util.UUID;

@RestController
@RequestMapping("/data")
@Slf4j
public class DataController {

    @Autowired
    private DataService dataService;

    /**
     * 在线比较 2 个数据源表数据
     *
     * @param compareData
     * @return
     */
    @PostMapping("/onlineCompare")
    public Object dataCompare(@RequestBody CompareData compareData) {
        try {
            log.info("入参：{}", compareData.toString());
            if (StringUtils.isEmpty(compareData.getSourceDataSource().getHost()) || StringUtils.isEmpty(compareData.getSourceDataSource().getPassword())
                    || StringUtils.isEmpty(compareData.getSourceDataSource().getDatabase()) || StringUtils.isEmpty(compareData.getSourceDataSource().getPort())
                    || StringUtils.isEmpty(compareData.getSourceDataSource().getUsername())) {
                return ResponseEntity.ok(Response.builder().code("10000").msg("源数据配置信息不能为空！").build());
            }
            if (StringUtils.isEmpty(compareData.getTargetDataSource().getHost()) || StringUtils.isEmpty(compareData.getTargetDataSource().getPassword())
                    || StringUtils.isEmpty(compareData.getTargetDataSource().getDatabase()) || StringUtils.isEmpty(compareData.getTargetDataSource().getPort())
                    || StringUtils.isEmpty(compareData.getTargetDataSource().getUsername())) {
                return ResponseEntity.ok(Response.builder().code("10000").msg("目标数据配置信息不能为空！").build());
            }
            if (CollectionUtils.isEmpty(compareData.getCompareLineList())) {
                return ResponseEntity.ok(Response.builder().code("10000").msg("比较配置行不能为空！").build());
            }
            Object obj = dataService.execCompare(compareData);
            if (obj == null) {
                return ResponseEntity.ok(Response.builder().code("10000").msg("服务异常！"));
            }

            return ResponseEntity.ok(Response.builder().code("0").msg("success").data(obj).build());
        } catch (Exception e) {
            log.error("执行数据比较异常！！！", e);
            return ResponseEntity.ok(Response.builder().code("10000").msg("执行数据比较异常，" + e.getMessage()).build());
        }
    }

    private String tempPath = System.getProperty("user.dir") + "/dataCompareTmp/";

    /**
     * 线下比较 2 个数据源表数据
     * 源数据库导出数据输出到 json 文件
     * 上传源库 json 文件，与目标数据库比较
     */
    @PostMapping("/oflineCompare/source")
    public Object offlineCompare(@RequestBody CompareData compareData) {
        log.info("入参：{}", compareData.toString());
        if (StringUtils.isEmpty(compareData.getSourceDataSource().getHost()) || StringUtils.isEmpty(compareData.getSourceDataSource().getPassword())
                || StringUtils.isEmpty(compareData.getSourceDataSource().getDatabase()) || StringUtils.isEmpty(compareData.getSourceDataSource().getPort())
                || StringUtils.isEmpty(compareData.getSourceDataSource().getUsername())) {
            return ResponseEntity.ok(Response.builder().code("10000").msg("源数据配置信息不能为空！").build());
        }
        if (StringUtils.isEmpty(compareData.getTargetDataSource().getHost()) || StringUtils.isEmpty(compareData.getTargetDataSource().getPassword())
                || StringUtils.isEmpty(compareData.getTargetDataSource().getDatabase()) || StringUtils.isEmpty(compareData.getTargetDataSource().getPort())
                || StringUtils.isEmpty(compareData.getTargetDataSource().getUsername())) {
            return ResponseEntity.ok(Response.builder().code("10000").msg("目标数据配置信息不能为空！").build());
        }
        if (CollectionUtils.isEmpty(compareData.getCompareLineList())) {
            return ResponseEntity.ok(Response.builder().code("10000").msg("比较配置行不能为空！").build());
        }
        // 源库，导出数据到json文件
        try {
            File tempFile = new File(tempPath + UUID.randomUUID() + ".json");
            log.info("临时目录：{}", tempFile.getAbsolutePath());
            if (!tempFile.getParentFile().exists()) {
                tempFile.getParentFile().mkdir();
            }
            dataService.importSourceData(compareData, tempFile);

            //设置头信息
            HttpHeaders headers = new HttpHeaders();
            headers.add("Cache-Control", "no-cache, no-store, must-revalidate");
            headers.add("Content-Disposition", "attachment; filename=sourceData.json");
            headers.add("Pragma", "no-cache");
            headers.add("Expires", "0");
            headers.add("Last-Modified", new Date().toString());
            headers.add("ETag", String.valueOf(System.currentTimeMillis()));
            return ResponseEntity.ok().headers(headers)
                    .contentLength(tempFile.length())
                    .contentType(MediaType.parseMediaType("application/octet-stream"))
                    .body(new FileSystemResource(tempFile));
        } catch (Exception e) {
            log.error("创建数据文件异常！！！", e);
            return ResponseEntity.ok(Response.builder().code("10000").msg("服务异常！").build());
        }
    }


    @PostMapping("/oflineCompare/target")
    public Object offlineTargetCompare(@RequestParam(value = "file") MultipartFile file) {
        if (file != null) {
            log.info("upload file name: {}, size: {}", file.getOriginalFilename(), file.getSize());
            if (!"json".equals(file.getOriginalFilename().substring(file.getOriginalFilename().lastIndexOf(".") + 1))) {
                return ResponseEntity.ok(Response.builder().code("10000").msg("文件格式不正确，请上传后缀为 json 文件！").build());
            }
        }
        CompareData compareData = null;
        try {
            // 解析源数据库 json 结果数据
            compareData = JSONObject.parseObject(new String(file.getBytes(), StandardCharsets.UTF_8), CompareData.class);
            log.info(compareData.toString());
        } catch (Exception e) {
            log.error("解析 json 数据异常！！！", e);
            return ResponseEntity.ok(Response.builder().code("10000").msg("json 解析异常！请核对数据！").build());
        }
        Object obj = dataService.compareWithJsonData(compareData);
        if (obj == null) {
            return ResponseEntity.ok(Response.builder().code("10000").msg("服务异常！").build());
        }

        return ResponseEntity.ok(Response.builder().code("0").msg("success").data(obj).build());
    }


    /**
     * 删除一天前的临时文件
     */
    @Scheduled(cron = "0 30 5 * * ?")
    public void cleanTempFiles() {
        try {
            File tempPathFile = new File(tempPath);
            if (!tempPathFile.exists()) {
                return;
            }
            File[] files = tempPathFile.listFiles();
            if (files != null && files.length > 0) {
                LocalDate lastDate = LocalDate.now().plusDays(-1);
                for (File file : files) {
                    Path path = file.toPath();
                    BasicFileAttributes attr = Files.readAttributes(path, BasicFileAttributes.class);
                    LocalDate createTime = attr.creationTime().toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
                    log.debug("file: {}, createTime: {}", file.getName(), createTime.toString());
                    if (createTime.isBefore(lastDate)) {
                        log.info("删除文件: {}", file.getAbsolutePath());
                        file.delete();
                    }
                }
            }
        } catch (IOException e) {
            log.error("删除临时文件异常！！！", e);
        }
    }
}
