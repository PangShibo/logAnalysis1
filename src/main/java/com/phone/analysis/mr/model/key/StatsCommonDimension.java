package com.phone.analysis.mr.model.key;

import com.phone.analysis.mr.model.base.BaseDimension;
import com.phone.analysis.mr.model.base.DateDimension;
import com.phone.analysis.mr.model.base.KpiDimension;
import com.phone.analysis.mr.model.base.PlatformDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * @Description
 * @Author cqh <caoqingghai@1000phone.com>
 * @Version V1.0
 * @Since 1.0
 * @Date 2019/1/4 14：50
 */
public class StatsCommonDimension extends StatsBaseDimension {
    private DateDimension dt = new DateDimension();
    private PlatformDimension pl = new PlatformDimension();
    private KpiDimension kpi = new KpiDimension();
    @Override
    public int compareTo(BaseDimension o) {
        if (this == o){
            return 0;
        }
        StatsCommonDimension other = (StatsCommonDimension)o;
        int tmp = this.dt.compareTo(other.dt);
        if (tmp != 0){
            return tmp;
        }
        tmp = this.pl.compareTo(other.pl);
        if (tmp != 0){
            return tmp;
        }
        return this.kpi.compareTo(other.kpi);
    }
    @Override
    public void write(DataOutput out) throws IOException {
        this.dt.write(out);
        this.pl.write(out);
        this.kpi.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.dt.readFields(in);
        this.pl.readFields(in);
        this.kpi.readFields(in);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StatsCommonDimension that = (StatsCommonDimension) o;
        return Objects.equals(dt, that.dt) &&
                Objects.equals(pl, that.pl) &&
                Objects.equals(kpi, that.kpi);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dt, pl, kpi);
    }

    public DateDimension getDt() {
        return dt;
    }

    public void setDt(DateDimension dt) {
        this.dt = dt;
    }

    public PlatformDimension getPl() {
        return pl;
    }

    public void setPl(PlatformDimension pl) {
        this.pl = pl;
    }

    public KpiDimension getKpi() {
        return kpi;
    }

    public void setKpi(KpiDimension kpi) {
        this.kpi = kpi;
    }

    @Override
    public String toString() {
        return "StatsCommonDimension{" +
                "dt=" + dt +
                ", pl=" + pl +
                ", kpi=" + kpi +
                '}';
    }
}