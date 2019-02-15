package com.phone.analysis.mr.model.value;

import com.phone.common.KpiType;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Description :reduce输出的value
 *
 */
public class MapWritableValue extends StatsOutputValue {
    private KpiType kpi; //标识
    private MapWritable value;  //存值

    public void setKpi(KpiType kpi) {
        this.kpi = kpi;
    }

    public MapWritable getValue() {
        return value;
    }

    public void setValue(MapWritable value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "MapWritableValue{" +
                "kpi=" + kpi +
                ", value=" + value +
                '}';
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeEnum(out,kpi);
        this.value.write(out);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        WritableUtils.readEnum(in, KpiType.class);
        this.value.readFields(in);
    }

    @Override
    public KpiType getKpi() {
        return this.kpi;
    }
}