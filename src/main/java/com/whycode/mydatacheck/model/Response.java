package com.whycode.mydatacheck.model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Response {
    private String code;
    private String msg;
    private Object data;
}
