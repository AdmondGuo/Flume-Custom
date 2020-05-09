package com.flume.interceptor.custominterceptor;

import com.alibaba.fastjson.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import oshi.SystemInfo;
import oshi.hardware.ComputerSystem;
import oshi.hardware.HardwareAbstractionLayer;
import oshi.software.os.OperatingSystem;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LofUtil {

    public static void main(String[] args) {
        Map<String, Object> metadat = new HashMap<>();
        Map<String, Object> headers = new HashMap<>();
        /*获得文件*/
        try {
            Yaml yaml = new Yaml();
            File file = new File("test.yml");
            if (file.exists()) {
                metadat = yaml.load(new FileInputStream(file));
            }
        } catch (Exception e) {
            if(e.getClass() .equals(FileNotFoundException.class)){
                System.out.println("1");
            }
            else if (e.getClass().equals(ClassCastException.class)){
                System.out.println("2");
            }
            else{
                System.out.println("3");
            }
        }
        List<String> list = new ArrayList<String>();
        List contentList = (List<String>)metadat.get("tags");
        String str = JSON.toJSON(contentList).toString();
        String strpure = str.substring(1,str.length()-1);
        System.out.println(strpure);
        String [] strlist = strpure.split(",");
        System.out.println(strlist[0]+" "+strlist[1]);
        headers.putIfAbsent("tags",contentList);
        System.out.println(headers);

        SystemInfo si = new SystemInfo();
        HardwareAbstractionLayer hw = si.getHardware();
        ComputerSystem computerSystem = hw.getComputerSystem();
        OperatingSystem os = si.getOperatingSystem();
        System.out.println(os.getFamily());
        System.out.println(computerSystem.getBaseboard());
    }

}
