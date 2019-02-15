package com.phone.etl.util;

import com.phone.common.Constants;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 解析日志信息工具类
 */

public class LogParseUtil {
    private static Logger logger=Logger.getLogger(LogParseUtil.class);

    /**
     * 解析一条日志，将键值对存入map集合返回
     */
    public static Map<String,String> parseLog(String log){
         Map<String,String> map=new ConcurrentHashMap<String, String>();
        if (StringUtils.isNotEmpty(log)){
            String [] fields=log.split("\\^A");
            if (fields.length==4){
                //日志格式为: ip^A服务器时间^Ahost^A请求参数
                //设置ip
                map.put(Constants.LOG_IP,fields[0]);
                //设置服务器时间
                map.put(Constants.LOG_SERVER_TIME,fields[1].replaceAll("\\.",""));
                String params=fields[3];
                //处理请求参数
                handleParams(params,map);
                //处理ip
                hadleIp(map);
                
                
            }
        }
        return map;
        
    }

    /**
     * 将map中的ip解析成地域信息，存储到map集合中
     * @param map
     */
    private static void hadleIp(Map<String, String> map) {
        if (map.containsKey(Constants.LOG_IP)){
            IpParseUtil.RegionInfo info =IpParseUtil.ipParse(map.get(Constants.LOG_IP));
            map.put(Constants.LOG_COUNTRY,info.getCountry());
            map.put(Constants.LOG_PROVINCE,info.getProvince());
            map.put(Constants.LOG_CITY,info.getCity());
            
        }
    }

    /**
     * 处理第四段参数参数
     * @param params
     * @param map
     */
    private static void handleParams(String params, Map<String, String> map) {
        if (StringUtils.isNotEmpty(params)){
            int index=params.indexOf("?");
            if (index>0){
                String [] fields =params.substring(index+1).split("&");
                try {
                    for (String field : fields) {
                        String[] kvs = field.split("=");
                        String k = kvs[0];
                        String v = URLDecoder.decode(kvs[1], "utf-8");
                        if (StringUtils.isNotEmpty(k)) {
                            map.put(k, v);
                        }
                    }
                }catch (UnsupportedEncodingException e) {
                        logger.error("url解码异常",e);
                    }

                }
                
            }
        }
    }
