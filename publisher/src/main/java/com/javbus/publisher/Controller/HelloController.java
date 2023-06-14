package com.javbus.publisher.Controller;

import com.javbus.publisher.bean.Country;
import org.springframework.web.bind.annotation.*;

/**
 * @Controller 类注解 将类标识为控制层
 * @ResponseBody 方法注解 将方法的返回值处理成字符串（json）返回给客户端
 * @RestController = @Controller + @ResponseBody
 */
//@Controller
@RestController
public class HelloController {

    /**
     * @RequestMapping 将请求和方法进行映射
     */
    @RequestMapping("finland")
    //@ResponseBody
    public String finland() {
        return "hello Finland!";
    }

    /**
     * 请求参数
     * 1. 地址栏中kv格式的参数
     * 2. 嵌入到地址栏中的参数
     * 3. 封装到请求体中的参数
     * <p>
     * kv格式的参数如果请求参数名和方法形参名一致 可以直接进行参数值的映射
     * http://localhost:8080/sweden?id=120&name=zhangsan
     */
    @RequestMapping("sweden")
    public String sweden(int id, String name) {
        return "id = " + id + ", name = " + name;
    }

    /**
     * 如果请求参数名和方法形参名不一致 使用@RequestParam进行映射
     * http://localhost:8080/norway?userid=5&name=zhangsan
     */
    @RequestMapping("norway")
    public String norway(@RequestParam("userid") int id, String name) {
        return "id = " + id + ", name = " + name;
    }

    /**
     * @PathVariable 针对嵌入到地址栏中的参数 将请求路径中的参数映射到请求方法对应的形参上
     * http://localhost:8080/iceland/1998/wangfei
     */
    @RequestMapping("iceland/{userid}/{username}")
    public String iceland(@PathVariable("userid") Integer id, @PathVariable("username") String name) {
        return "id = " + id + ", name = " + name;
    }

    /**
     * 使用postman将参数封装到请求体中 (POST)方式
     * {
     * "name": "The Kingdom of Denmark",
     * "capital": "Copenhagen",
     * "population": 592.8,
     * "area": 43096
     * }
     *
     * @RequestBody 将请求体中json格式的参数映射到参数对象的属性上
     */
    @RequestMapping("denmark")
    public Country denmark(@RequestBody Country country) {
        return country;
    }

    /**
     * 请求方式
     * GET  一般用来读
     * POST 一般用来写
     * <p>
     * 可以指定请求方式
     */
    @RequestMapping(value = "britain", method = RequestMethod.GET)
    public String britain() {
        return "hello Britain!";
    }

    @GetMapping("holland")
    public String holland() {
        return "hello Holland!";
    }

    /**
     * 状态码
     * 200 请求成功且相应成功
     * 302 重定向
     * 400 请求参数有误
     * 404 请求的地址或资源不存在
     * 405 请求方式不支持
     * 500 服务器处理异常
     *
     */


}
