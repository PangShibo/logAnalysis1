package com.phone.etl.mr;

import com.phone.common.Constants;
import com.phone.etl.util.LogParseUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import java.util.Map;

public class EtlToHdfsMapper extends Mapper<LongWritable, Text,LogWritable, NullWritable>{
    private static Logger logger=Logger.getLogger(EtlToHdfsMapper.class);
    
    //封装map端输出的key
    private static LogWritable k =new LogWritable();
    //记录输入了多少条数据，过滤了多少条数据，输出了多少条数据
    private static int inputRecords,filterRecords,outputRecords;
    
    
    protected void map(LongWritable key,Text value,Context context){
        String log=value.toString();
        inputRecords++;
        if (StringUtils.isEmpty(log)){
            filterRecords++;
            return;
        }
        //将解析的数据返回一个map集合
        Map<String, String> map = LogParseUtil.parseLog(log);
        //给k赋值
        String eventName=map.get(Constants.LOG_EVENT_NAME);
        //获取事件类型
        Constants.EventEnum eventEnum=Constants.EventEnum.valueOfAlias(eventName);
        switch (eventEnum){
            case LANUCH:
            case EVENT:
            case CHARGESUCCESS:
            case CHARGEREFUND:
            case CHARGEREQUEST:
            case PAGEVIEW:
                //将map中数据取出来赋给k，调用context.write写出
                handle(map,context);
                break;
                default:
                    break;
        }
        

    }

    /**
     * 将map中数据取出来，赋给k，然后输出
     * @param map
     * @param context
     */

    private void handle(Map<String, String> map, Context context) {
        for (Map.Entry<String,String>en :map.entrySet()){
            switch (en.getKey()){
                case "ver" :this.k.setVer(en.getValue());break;
                case "s_time" : this.k.setS_time(en.getValue()); break;
                case "en" : this.k.setEn(en.getValue()); break;
                case "u_ud" : this.k.setU_ud(en.getValue()); break;
                case "u_mid" : this.k.setU_mid(en.getValue()); break;
                case "u_sd" : this.k.setU_sd(en.getValue()); break;
                case "c_time" : this.k.setC_time(en.getValue()); break;
                case "l" : this.k.setL(en.getValue()); break;
                case "b_iev" : this.k.setB_iev(en.getValue()); break;
                case "b_rst" : this.k.setB_rst(en.getValue()); break;
                case "p_url" : this.k.setP_url(en.getValue()); break;
                case "p_ref" : this.k.setP_ref(en.getValue()); break;
                case "tt" : this.k.setTt(en.getValue()); break;
                case "pl" : this.k.setPl(en.getValue()); break;
                case "ip" : this.k.setIp(en.getValue()); break;
                case "oid" : this.k.setOid(en.getValue()); break;
                case "on" : this.k.setOn(en.getValue()); break;
                case "cua" : this.k.setCua(en.getValue()); break;
                case "cut" : this.k.setCut(en.getValue()); break;
                case "pt" : this.k.setPt(en.getValue()); break;
                case "ca" : this.k.setCa(en.getValue()); break;
                case "ac" : this.k.setAc(en.getValue()); break;
                case "kv_" : this.k.setKv_(en.getValue()); break;
                case "du" : this.k.setDu(en.getValue()); break;
                case "browserName" : this.k.setBrowserName(en.getValue()); break;
                case "browserVersion" : this.k.setBrowserVersion(en.getValue()); break;
                case "osName" : this.k.setOsName(en.getValue()); break;
                case "osVersion" : this.k.setOsVersion(en.getValue()); break;
                case "country" : this.k.setCountry(en.getValue()); break;
                case "province" : this.k.setProvince(en.getValue()); break;
                case "city" : this.k.setCity(en.getValue()); break;
                
                
            }
            
        }
        try {
            context.write(k,NullWritable.get());
            outputRecords++;
        } catch (Exception e) {
            logger.error("etl输出异常",e);
        } 
    }
    
protected void cleanup(Context context){
        logger.info("输入："+inputRecords+"输出："+outputRecords+"过滤："+filterRecords);
}

}
