package com.phone.analysis.mr.nu;

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

public class NewUserReducer extends Reducer<StatsUserDimension, TimeOutputValue,StatsUserDimension, OutputWritable> {
    private Logger logger=Logger.getLogger(NewUserReducer.class);
    private OutputWritable v = new OutputWritable();
    private Set unique = new HashSet();//用于去重，利用HashSet
    private MapWritable map = new MapWritable();
    
    protected void reduce(StatsUserDimension key,Iterable<TimeOutputValue> values,Context context) throws IOException, InterruptedException {
        map.clear();
        //将uuid取出来放到set去重
        for (TimeOutputValue timeOutputValue : values) {
            this.unique.add(timeOutputValue.getId());
        }
        //构造输出的value
        this.v.setKpi(KpiType.valueOfKpiName(key.getStatsCommonDimension().getKpi().getKpiName()));
        //通过set的size确定uuid个数
        this.map.put(new IntWritable(-1),new IntWritable(this.unique.size()));
        this.v.setValue(this.map);
        //输出
        context.write(key,this.v);
        this.unique.clear();//清空操作


    }
    
    


}
