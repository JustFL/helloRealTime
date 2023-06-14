package com.javbus.publisher.Controller;

import com.javbus.publisher.service.DauService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;

@RestController
public class PublisherController {

    // 面对接口编程 自动注入时需要声明为@service 如果有多个类声明 则可以使用@Qualifier显示指定哪个类注入
    @Autowired
    DauService dauService;

    // http://localhost/dauRealtime?dt=2022-01-01
    @RequestMapping("dauRealtime")
    public HashMap<String, Object> dauRealtime(@RequestParam("dt") String dt) {
        return dauService.acquireDau(dt);
    }
}
