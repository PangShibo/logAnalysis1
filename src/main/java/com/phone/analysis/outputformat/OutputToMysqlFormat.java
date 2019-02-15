package com.phone.analysis.outputformat;

import com.phone.analysis.mr.model.key.StatsBaseDimension;
import com.phone.analysis.mr.model.value.StatsOutputValue;
import com.phone.analysis.service.IDimension;
import com.phone.analysis.service.impl.IDimensionImpl;
import com.phone.common.KpiType;
import com.phone.util.JdbcUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * @Description :自定义输出到mysql的格式化类
 * @Author cqh <caoqingghai@1000phone.com>
 * @Version V1.0
 * @Since 1.0
 * @Date 2019/1/5 14：19
 */
public class OutputToMysqlFormat extends OutputFormat<StatsBaseDimension, StatsOutputValue> {
   //获取RecordWriter子类对象，子类对象中有负责写出的write(k,v)方法
    @Override
    public RecordWriter getRecordWriter(TaskAttemptContext context)  {
        Connection conn = JdbcUtil.getConn();
        Configuration conf = context.getConfiguration();
        IDimension iDimension = new IDimensionImpl();
        return new MysqlRecordWriter(conn,conf,iDimension);
    }

    @Override
    public void checkOutputSpecs(JobContext context) {
        //do nothing
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException {
        return new FileOutputCommitter(FileOutputFormat.getOutputPath(context),
                context);
    }
    public static class MysqlRecordWriter extends RecordWriter<StatsBaseDimension, StatsOutputValue>{
        private Connection conn = null;
        private Configuration conf = null;
        IDimension iDimension = null;
        //缓存ps语句 kpi:ps
        private Map<KpiType, PreparedStatement> map = new HashMap<KpiType, PreparedStatement>();
        //存储kpi对应的要处理的数据量
        //当该kpi对应的数据数量达到一定值时，批量执行
        //这个map就是做计数用的
        private Map<KpiType, Integer> batch = new HashMap<KpiType, Integer>();
        public MysqlRecordWriter() {
        }

        public MysqlRecordWriter(Connection conn, Configuration conf, IDimension iDimension) {
            this.conn = conn;
            this.conf = conf;
            this.iDimension = iDimension;
        }

        /**
         * 真正写出到数据库的方法
         * @param key
         * @param value
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void write(StatsBaseDimension key, StatsOutputValue value) {
            //从value中获取KPI eg：NEW_USER("new_user")
            //StatsUserDimension MapWritableOutputvale
            //2018-11-11 IOS NEW_USER : {NEW_USER,[1:2000]}
            KpiType kpi = value.getKpi();
            PreparedStatement ps = null;
            //判断缓存中是否有该KPI对应的ps
            int count = 1;
            try {
                if (map.containsKey(kpi)){
                    //map kpi:ps
                    ps = map.get(kpi);
                    count = batch.get(kpi);
                    count++;
                    //batch.put(kpi,count);
                }else {
                    //代码运行到这里说明缓存中没有该kpi对应的ps
                    //获取kpi对应的sql语句
                    //System.out.println("*************"+kpi.kpiName);
                    String sql = conf.get(kpi.kpiName);
                    //获取ps对象
                    ps = conn.prepareStatement(sql);

                    //存入缓存
                    map.put(kpi,ps);
                    //batch.put(kpi,count);
                }
                batch.put(kpi,count);
                //为ps赋值
               //kpi不同，sql就不同，赋值的方法就不同
                //做一个接口，接口中包含一个抽象方法，这个方法就是给语句赋值的
                //所有给语句赋值的子类实现这个接口
                //根据kpi的不同获取不同的子类来给语句赋值
                //把类名写入conf中，kpi不同获取到不同的类名
                //使用反射获取到该类

                //通过conf获取类名 包名+类名
                //com.phone.analysis.mr.nu.NewUserOutputWriter
                String className = conf.get("writter_"+kpi.kpiName);
                //通过反射获取到这个类
                Class<?>classz = Class.forName(className);
                //获取这个类的一个实例对象
                IOutputValue iOutputValue = (IOutputValue)classz.newInstance();
                //用iOutputValue调用赋值的方法
                iOutputValue.output(key,value,iDimension,ps,conf);

                //判断该kpi对应的数据数量是否达到50条，如果达到就执行ps
                if (batch.get(kpi)%50==0){
                    //批量执行
                    ps.executeBatch();
                    //重新计数
                    batch.put(kpi,0);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        //NEW_USER:ps1
        //BROWSER_NEW_USER:ps2
        @Override
        public void close(TaskAttemptContext context){
            //NEW_USER:ps1
            //BROWSER_NEW_USER:ps2
            //循环执行缓存中的ps
            try {
                for (Map.Entry<KpiType,PreparedStatement> en : map.entrySet()){
                    en.getValue().executeBatch();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                //NEW_USER:ps1
                //BROWSER_NEW_USER:ps2
                //循环关闭
                for (Map.Entry<KpiType,PreparedStatement> en : map.entrySet()){
                    JdbcUtil.close(conn,en.getValue(),null);
                }
            }
        }
    }
}