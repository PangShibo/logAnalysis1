package com.phone.analysis.mr.nm;

import com.phone.analysis.mr.model.key.StatsUserDimension;
import com.phone.analysis.mr.model.value.OutputWritable;
import com.phone.analysis.mr.model.value.TimeOutputValue;
import com.phone.analysis.outputformat.OutputToMysqlFormat;
import com.phone.common.GlobalConstants;
import com.phone.util.TimeUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;


public class NewMemberRunner implements Tool {
    private static final Logger logger = Logger.getLogger(NewMemberRunner.class);
    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(),new NewMemberRunner(),args);
        } catch (Exception e) {
            logger.warn("运行new member runner 异常.",e);
        }
    }

    private Configuration conf = null;
    @Override
    public void setConf(Configuration conf) {
        //????????????
        conf.addResource("output-mapping.xml");
        conf.addResource("output-writter.xml");
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    /**
     * yarn jar ./*.jar com.mobile.etl.tohdfs.ToHdfsRunner -d 2018-12-04
     * /logs/12/04
     */

    @Override
    public int run(String[] args) throws Exception {
        //获取配置
        Configuration conf = this.getConf();
        //处理参数，会将运行日期存储到conf中
        setArg(args,conf);
        //获取job
        Job job= Job.getInstance(conf,"new user");

        job.setJarByClass(NewMemberRunner.class);
        job.setMapperClass(NewMemberMapper.class);
        job.setMapOutputKeyClass(StatsUserDimension.class);
        job.setMapOutputValueClass(TimeOutputValue.class);

        job.setReducerClass(NewMemberReducer.class);
        job.setOutputKeyClass(StatsUserDimension.class);
        job.setOutputValueClass(OutputWritable.class);

        //设置输入输出参数
        handleInputAndOutput(job);

        //设置输出格式类
        job.setOutputFormatClass(OutputToMysqlFormat.class);

       return job.waitForCompletion(true) ? 0 : 1;
    }


    /**
     *将运行的date设置到conf中
     * @param args
     */
    private void setArg(String[] args,Configuration conf) {
        for (int i = 0;i < args.length;i++){
            if(args[i].equals("-d")){
                if(i+1 < args.length){
                    conf.set(GlobalConstants.RUNNING_DATE,args[i+1]);
                    break;  //跳出整个循环
                }
            }
        }

        //判断conf中是否存在
        if(conf.get(GlobalConstants.RUNNING_DATE) == null){
            //给默认值
            conf.set(GlobalConstants.RUNNING_DATE, TimeUtil.getYesterday());
        }
    }

    /**
     * 设置输入输出参数
     * @param job
     */
    private void handleInputAndOutput(Job job) {
        String date = job.getConfiguration().get(GlobalConstants.RUNNING_DATE);
        String fields[] = date.split("-");
        ///logs/12/04
        try {
            Path inputpath = new Path("/ods/month="+fields[1]+"/day="+fields[2]);
            FileInputFormat.setInputPaths(job,inputpath);
        } catch (IOException e) {
            logger.warn("设置输入输出路径异常",e);
        }
    }
}