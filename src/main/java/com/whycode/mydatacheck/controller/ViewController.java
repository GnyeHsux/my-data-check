package com.whycode.mydatacheck.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
public class ViewController {

    @RequestMapping("/index")
    public String online(){
        return "/pages/index.html";
    }

    @RequestMapping("/offline")
    public String offline(){
        return "/pages/offline.html";
    }
}
