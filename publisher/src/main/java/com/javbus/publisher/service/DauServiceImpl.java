package com.javbus.publisher.service;

import com.javbus.publisher.mapper.DauMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;

@Service
public class DauServiceImpl implements DauService {

    @Autowired
    DauMapper publisherMapper;

    @Override public HashMap<String, Object> acquireDau(String dt) {
        return publisherMapper.gainDau(dt);
    }
}
