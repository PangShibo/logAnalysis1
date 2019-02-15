package com.phone.analysis.mr.model.base;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * @Description : kpi维度类
 * @Author cqh <caoqingghai@1000phone.com>
 * @Version V1.0
 * @Since 1.0
 * @Date 2019/1/4 11：42
 */
public class KpiDimension extends BaseDimension {
    private int id;
    private String kpiName;

    public KpiDimension() {
    }

    public KpiDimension(String kpiName) {
        this.kpiName = kpiName;
    }

    public KpiDimension(int id, String kpiName) {
        this.id = id;
        this.kpiName = kpiName;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getKpiName() {
        return kpiName;
    }

    public void setKpiName(String kpiName) {
        this.kpiName = kpiName;
    }

    @Override
    public String toString() {
        return "KpiDimension{" +
                "id=" + id +
                ", kpiName='" + kpiName + '\'' +
                '}';
    }
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KpiDimension that = (KpiDimension) o;
        return id == that.id &&
                Objects.equals(kpiName, that.kpiName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, kpiName);
    }

    @Override
    public int compareTo(BaseDimension o) {
        if (this == o){
            return 0;
        }
        KpiDimension other = (KpiDimension)o;
        int tmp = this.id -other.id;
        if (tmp != 0){
            return tmp;
        }
        return this.kpiName.compareTo(other.kpiName);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.id);
        out.writeUTF(this.kpiName);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readInt();
        this.kpiName = in.readUTF();
    }
}