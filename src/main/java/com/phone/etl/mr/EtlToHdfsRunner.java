package com.phone.etl.mr;

import com.phone.common.GlobalConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;


public class EtlToHdfsRunner implements Tool {
    private static Logger logger=Logger.getLogger(EtlToHdfsRunner.class);
    private Configuration conf=new Configuration();
    //放job设置
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf=getConf();
        //将参数存储到conf中
        handleArgs(conf,args);
        Job job=Job.getInstance(conf);
        job.setJarByClass(EtlToHdfsRunner.class);
        job.setMapperClass(EtlToHdfsMapper.class);
        job.setMapOutputKeyClass(LogWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        //设置reduce数量为0
        job.setNumReduceTasks(0);
        //设置输入输出路径
        handleInputOutput(job);
        return job.waitForCompletion(true)?0:1;
        
    }

    private void handleInputOutput(Job job) {
        FileSystem fs = null;
        try {
            String date = job.getConfiguration().get(GlobalConstants.RUNNING_DATE);
            String [] fields = date.split("-");
            String month = fields[1];
            String day = fields[2];
            fs = FileSystem.get(job.getConfiguration());
            Path in = new Path("/logs/"+month+"/"+day);
            Path out = new Path("/ods/"+month+"/"+day);
            //设置输入路径
            if (fs.exists(in)){
                FileInputFormat.addInputPath(job,in);
            }else{
                throw new RuntimeException("输入路径不存在"+in);
            }
            //设置输出路径
            if(fs.exists(out)){
                fs.delete(out,true);
            }
            FileOutputFormat.setOutputPath(job,out);
        } catch (IOException e) {
            logger.error("设置输入输出路径异常",e);
        } finally {
            if (fs!=null){
                try {
                    fs.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }
    }

    /**
     * 从参数中获取时间，将时间存到conf中
     * @param conf
     * @param args
     */
    private void handleArgs(Configuration conf,String[] args){
        String date = null;
        if(args.length>0){
            for (int i=0;i<args.length;i++){
                if (args[i].equals("-d")){
                    if (i+1<=args.length){
                        date = args[i+1];
                        break;
                    }
                }
            }
        }
        
        conf.set(GlobalConstants.RUNNING_DATE,date);
    }
    

    @Override
    public void setConf(Configuration config) {
        config.addResource("core-site.xml");
        this.conf=config;
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(),new EtlToHdfsRunner(),args);
        } catch (Exception e) {
            logger.error("etl异常",e);
        }
    }
}
