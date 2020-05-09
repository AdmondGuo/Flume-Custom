package com.flume.interceptor.custominterceptor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.net.InetAddress;
import java.util.*;

import com.alibaba.fastjson.JSON;
import org.yaml.snakeyaml.Yaml;
import oshi.SystemInfo;
import oshi.hardware.ComputerSystem;
import oshi.hardware.HardwareAbstractionLayer;

import static org.apache.flume.interceptor.HostInterceptor.Constants.PRESERVE_DFLT;

/**
 * 自定义拦截器，实现Interceptor接口，并且实现其抽象方法
 */
public class CustomInterceptor implements Interceptor {

    //打印日志，便于测试方法的执行顺序
    private static final Logger logger = LoggerFactory.getLogger(CustomInterceptor.class);
    //自定义拦截器参数，用来接收自定义拦截器flume配置参数
    private static String param = "";


    /**
     * rank:3
     * 拦截器构造方法，在自定义拦截器静态内部类的build方法中调用，用来创建自定义拦截器对象。
     */
    public CustomInterceptor() {
        logger.info("----------自定义拦截器构造方法执行");
    }


    /**
     * rank:4
     * 该方法用来初始化拦截器，在拦截器的构造方法执行之后执行，也就是创建完拦截器对象之后执行
     */
    @Override
    public void initialize() {
        logger.info("----------自定义拦截器的initialize方法执行");
    }

    /**
     * rank:6
     * 用来处理每一个event对象，该方法不会被系统自动调用，一般在 List<Event> intercept(List<Event> events) 方法内部调用。
     *
     * @param event
     * @return
     */
    @Override
    public Event intercept(Event event) {
        logger.info("----------intercept(Event event)方法执行，处理单个event");
        SystemInfo si = new SystemInfo();
        HardwareAbstractionLayer hw = si.getHardware();
        ComputerSystem computerSystem = hw.getComputerSystem();
        Map<String, String> headers = event.getHeaders();
        Map<String, Object> metadat = null;
        /*获得文件*/
        try {
            Yaml yaml = new Yaml();
            File file = new File("C:\\Users\\edz\\Desktop\\flume+es\\apache-flume-1.9.0-bin\\conf\\test.yml");
            if (file.exists()) {
                metadat = yaml.load(new FileInputStream(file));
            }
        } catch (Exception e) {
            if(e.getClass() .equals(FileNotFoundException.class)){
                logger.error("something wrong when get meta from yaml");
            }
            else if (e.getClass().equals(ClassCastException.class)){
                logger.error("Please check your yaml configuration!");
            }
            else{
                logger.error("未知错误");
            }
        }
        /*获得系统信息*/
        try {
            /* host json*/
            JSONObject hostjson = new JSONObject();
            JSONObject osjson = new JSONObject();
            String ihostname = InetAddress.getLocalHost().getHostName(); // 获得hostname
            /*meta数据默认值*/
            hostjson.put("name", metadat.getOrDefault("name",ihostname));
            hostjson.put("hostname", ihostname);
            hostjson.put("architecture", System.getProperty("os.arch"));
            hostjson.put("id", UUID.randomUUID().toString());

            //os data
            osjson.put("platform", System.getProperty("os.name"));
            osjson.put("version", System.getProperty("os.version"));
            osjson.put("family", System.getProperty("os.name").contains("Win")?"windows":"linix");
            osjson.put("name", System.getProperty("os.name"));
            osjson.put("kernel", computerSystem.getBaseboard().getModel());
            osjson.put("build", computerSystem.getBaseboard());
            //formulate os & host
            hostjson.put("os", osjson);
            headers.put("host", hostjson.toJSONString());
            /* agent */
            JSONObject agentjson = new JSONObject();
            agentjson.put("ephemeral_id", UUID.randomUUID().toString());
            agentjson.put("hostname", ihostname);
            agentjson.put("id", UUID.randomUUID().toString());
            agentjson.put("version", "7.6.2");
            agentjson.put("type", "flume");
            headers.put("agent", agentjson.toJSONString());
            /* log */
            JSONObject logjson = new JSONObject();
            JSONObject filejson = new JSONObject();
            filejson.put("path", "C:\\opt\\orchestrator\\agent\\dat\\logs\\agent.log");
            logjson.put("file", filejson);

            logjson.put("offset", Integer.parseInt(headers.get("byteoffset")));
            headers.put("log", logjson.toJSONString());
            headers.remove("byteoffset");
            /*tags*/
            headers.put("tags", metadat.get("tags").toString());
            /*field.index*/
            JSONObject indexjson = new JSONObject();
            indexjson.put("index",metadat.get("index"));
            headers.put("fields", indexjson.toJSONString());
            /*input.type*/
            JSONObject typejson = new JSONObject();
            typejson.put("type", metadat.get("type"));
            headers.put("input", typejson.toJSONString());
        } catch (Exception e) {
            logger.error("something is wrong: "+e);
        }
        return event;
    }

    /**
     * rank:5
     * 用来处理一批event对象集合，集合大小与flume启动配置有关，和transactionCapacity大小保持一致。一般直接调用 Event intercept(Event event) 处理每一个event数据。
     *
     * @param events
     * @return
     */
    @Override
    public List<Event> intercept(List<Event> events) {

        logger.info("----------intercept(List<Event> events)方法执行");

        /*
        这里编写对于event对象集合的处理代码，一般都是遍历event的对象集合，对于每一个event对象，调用 Event intercept(Event event) 方法，然后根据返回值是否为null，
        来将其添加到新的集合中。
         */
        List<Event> results = new ArrayList<>();
        Event event;
        for (Event e : events) {
            event = intercept(e);
            if (event != null) {
                results.add(event);
            }
        }
        return results;
    }

    /**
     * 该方法主要用来销毁拦截器对象值执行，一般是一些释放资源的处理
     */
    @Override
    public void close() {
        logger.info("----------自定义拦截器close方法执行");
    }

    /**
     * 通过该静态内部类来创建自定义对象供flume使用，实现Interceptor.Builder接口，并实现其抽象方法
     */
    public static class Builder implements Interceptor.Builder {
        /**
         * rank:2
         * 该方法主要用来返回创建的自定义类拦截器对象
         * @return
         */
        @Override
        public Interceptor build() {
            logger.info("----------build方法执行");
            return new CustomInterceptor();
        }

        /**
         * rank：1
         * 用来接收flume配置自定义拦截器参数
         * @param context 通过该对象可以获取flume配置自定义拦截器的参数
         */
        @Override
        public void configure(Context context) {
            logger.info("----------configure方法执行");
            /*
            通过调用context对象的getString方法来获取flume配置自定义拦截器的参数，方法参数要和自定义拦截器配置中的参数保持一致+
             */
            param = context.getString("param");
        }
    }
}
