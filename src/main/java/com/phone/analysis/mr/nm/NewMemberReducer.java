package com.phone.analysis.mr.nm;


import com.phone.analysis.mr.model.key.StatsUserDimension;
import com.phone.analysis.mr.model.value.OutputWritable;
import com.phone.analysis.mr.model.value.TimeOutputValue;
import com.phone.common.KpiType;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * @ClassName NewUserReducer
 * @Author lyd
 * @Date $ $
 * @Vesion 1.0
 * @Description 新增会员的reducer类
 **/
public class NewMemberReducer extends Reducer<StatsUserDimension, TimeOutputValue,
        StatsUserDimension, OutputWritable> {
    private static final Logger logger = Logger.getLogger(NewMemberReducer.class);
    private OutputWritable v = new OutputWritable();
    private Set<String> unique = new HashSet<String>(); //用于存储uuid来去重
    
    protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {
        //清空set
        this.unique.clear();
        //循环
        for (TimeOutputValue mo:values){
            this.unique.add(mo.getId()); //存储uuid
        }

        //构造输出的value
        MapWritable map = new MapWritable();
        //-1 代表是新增用户
        map.put(new IntWritable(-1),new IntWritable(this.unique.size()));
        this.v.setValue(map);
        //根据kpi判断设置
//        this.v.setKpi(KpiType.valueOf(key.getStatsCommonDimension().getKpiDiemension().getKpiName()));
       if(KpiType.NEW_MEMBER.kpiName.equals(key.getStatsCommonDimension().getKpi().getKpiName())){
            this.v.setKpi(KpiType.NEW_MEMBER);
        } else if(KpiType.BROWSER_NEW_MEMBER.kpiName.equals(key.getStatsCommonDimension().getKpi().getKpiName())){
            this.v.setKpi(KpiType.BROWSER_NEW_MEMBER);
        }
        //输出
        context.write(key,this.v);
    }
}