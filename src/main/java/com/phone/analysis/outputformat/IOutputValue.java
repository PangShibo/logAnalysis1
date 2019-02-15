package com.phone.analysis.outputformat;

import com.phone.analysis.mr.model.key.StatsBaseDimension;
import com.phone.analysis.mr.model.value.StatsOutputValue;
import com.phone.analysis.service.IDimension;
import org.apache.hadoop.conf.Configuration;

import java.sql.PreparedStatement;

/**
 * @Description ：操作表的接口
 * @Author cqh <caoqingghai@1000phone.com>
 * @Version V1.0
 * @Since 1.0
 * @Date 2019/1/5 15：19
 */
public interface IOutputValue {
    void output(StatsBaseDimension key,//获取维度对象
                StatsOutputValue value,//获取value存的值
                IDimension iDimension,//通过维度对象获取维度ID
                PreparedStatement ps,//ps对象
                Configuration conf//获取running_date
    );
}