package com.phone.analysis.mr.model.value;

import com.phone.common.KpiType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *
 * 浏览器模块下map输出的value
 * String id;
 * long time;
 */
public class TimeOutputValue extends StatsOutputValue {
    private String id;//uuid,u_mid
    private long time;

    @Override
    public String toString() {
        return "TimeOutputValue{" +
                "id='" + id + '\'' +
                ", time=" + time +
                '}';
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.id);
        out.writeLong(this.time);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readUTF();
        this.time = in.readLong();
    }

    @Override
    public KpiType getKpi() {
        return null;
    }
}