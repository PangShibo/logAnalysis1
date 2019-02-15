package com.phone.analysis.mr.model.value;

import com.phone.common.KpiType;
import org.apache.hadoop.io.Writable;

/**
 * @Description :输出的value的顶级父类
 * 规定了所有输出的value要实现序列化和反序列化
 * @Author cqh <caoqingghai@1000phone.com>
 * @Version V1.0
 * @Since 1.0
 * @Date 2019/1/4 14：59
 */
public abstract class StatsOutputValue implements Writable {
    //获取kpi的抽象方法
    public abstract KpiType getKpi();
}