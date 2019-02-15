/**
 * Copyright (C), 2015-2018, XXX有限公司
 * FileName: NewUserWritter
 * Author:   14751
 * Date:     2018/9/21 23:43
 * Description:
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间         版本号            描述
 */
package com.phone.analysis.mr.au;

import com.phone.analysis.mr.model.key.StatsBaseDimension;
import com.phone.analysis.mr.model.key.StatsUserDimension;
import com.phone.analysis.mr.model.value.OutputWritable;
import com.phone.analysis.mr.model.value.StatsOutputValue;
import com.phone.analysis.outputformat.IOutputValue;
import com.phone.analysis.service.IDimension;
import com.phone.common.GlobalConstants;
import com.phone.common.KpiType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.log4j.Logger;

import java.sql.PreparedStatement;

/**
 * 活跃用户的sql赋值
 */
public class ActiveUserOutputWritter implements IOutputValue {
    private static final Logger logger = Logger.getLogger(ActiveUserOutputWritter.class);
  
    //这里通过key和value给ps语句赋值
    @Override
    public void output(StatsBaseDimension key, StatsOutputValue value, IDimension iDimension, PreparedStatement ps, Configuration conf) {

        try {
            StatsUserDimension k = (StatsUserDimension) key;
            OutputWritable v = (OutputWritable) value;

            int i = 0;

            switch (v.getKpi()){
                case ACTIVE_USER:
                case BROWSER_ACTIVE_USER:
                    //获取活跃用户的值
                    int newUser = ((IntWritable)(v.getValue().get(new IntWritable(-1)))).get();

                    ps.setInt(++i,iDimension.getIDimensionByObject(k.getStatsCommonDimension().getDt()));
                    ps.setInt(++i,iDimension.getIDimensionByObject(k.getStatsCommonDimension().getPl()));
                    //修改1
                    if(v.getKpi().equals(KpiType.BROWSER_ACTIVE_USER)){
                        ps.setInt(++i,iDimension.getIDimensionByObject(k.getBrowserDimension()));
                    }
                    ps.setInt(++i,newUser);
                    ps.setString(++i,conf.get(GlobalConstants.RUNNING_DATE));//注意这里需要在runner类里面进行赋值
                    ps.setInt(++i,newUser);
                    break;

                case HOURLY_ACTIVE_USER:
                    ps.setInt(++i,iDimension.getIDimensionByObject(k.getStatsCommonDimension().getDt()));
                    ps.setInt(++i,iDimension.getIDimensionByObject(k.getStatsCommonDimension().getPl()));
                    ps.setInt(++i,iDimension.getIDimensionByObject(k.getStatsCommonDimension().getKpi()));

                    for (int j = 0;j<24;j++){
                        ps.setInt(++i,((IntWritable)(v.getValue().get(new IntWritable(j)))).get());
                    }
                    ps.setString(++i,conf.get(GlobalConstants.RUNNING_DATE));
                    for (int j = 0;j<24;j++){
                        ps.setInt(++i,((IntWritable)(v.getValue().get(new IntWritable(j)))).get());
                    }
                    break;
            }


            ps.addBatch();//添加到批处理中，批量执行SQL语句
        } catch (Exception e) {
            logger.warn("给ps赋值失败！！！",e);
        }
    }
}