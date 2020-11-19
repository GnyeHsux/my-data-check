package com.whycode.mydatacheck.service;

import com.whycode.mydatacheck.model.online.CompareData;

import java.io.File;

public interface DataService {

    Object execCompare(CompareData compareData) throws Exception;

    void importSourceData(CompareData compareData, File tempFile) throws Exception;

    Object compareWithJsonData(CompareData compareData);
}
