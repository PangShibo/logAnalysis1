package com.phone.util;

import com.phone.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @description
 * @author: 王乾坤
 * @create: 2019-01-08 09:22:07
 **/
public class InAndOutPathUtil {
  /**
   * 设置输入输出路径的方法
   *
   * @param logger
   * @param job
   */
  public static void handleInputOutput(Logger logger, Job job) {
    FileSystem fs = null;
    try {
      String date = job.getConfiguration().get(GlobalConstants.RUNNING_DATE);
      //2018-11-11 2018 11 11
      String[] fields = date.split("-");
      String month = fields[1];
      String day = fields[2];
      // 获取到hdfs文件系统
      fs = FileSystem.get(job.getConfiguration());
      Path in = new Path("/ods/" + month + "/" + day);
      // 设置输入路径
      if (fs.exists(in)) {
        FileInputFormat.addInputPath(job, in);
      } else {
        throw new RuntimeException("输入路径不存在" + in);
      }

    } catch (IOException e) {
      logger.error("设置输入输出路径异常", e);
    } finally {
      if (fs != null) {
        try {
          fs.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

  /**
   * 从参数中获取时间，将时间存入conf中
   *
   * @param conf
   * @param args
   */
  public static void handleArgs(Configuration conf, String[] args) {
    String date = null;
    if (args.length > 0) {
      for (int i = 0; i < args.length; i++) {
        if (args[i].equals("-d")) {
          if (i + 1 <= args.length) {
            date = args[i + 1];
            break;
          }
        }
      }
    }
    //yarn jar /xx/xc/css/ss.jar xxx.xx.xx
    //如果没有时间参数我们可以给一个默认值
    //默认时间是前一天的时间
    if (StringUtils.isEmpty(date)) {
      date = TimeUtil.getYesterday();
    }
    conf.set(GlobalConstants.RUNNING_DATE, date);
  }
}
