package com.phone.etl.mr;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 封装map输出的key
 */

public class LogWritable implements Writable {
    private String ver;
    private String s_time;
    private String en;
    private String u_ud;
    private String u_mid;
    private String u_sd;
    private String c_time;
    private String l;
    private String b_iev;
    private String b_rst;
    private String p_url;
    private String p_ref;
    private String tt;
    private String pl;
    private String ip;
    private String oid;
    private String on;
    private String cua;
    private String cut;
    private String pt;
    private String ca;
    private String ac;
    private String kv_;
    private String du;
    private String browserName;
    private String browserVersion;
    private String osName;
    private String osVersion;
    private String country;
    private String province;
    private String city;

    public String getVer() {
        return ver;
    }

    public void setVer(String ver) {
        this.ver = ver;
    }

    public String getS_time() {
        return s_time;
    }

    public void setS_time(String s_time) {
        this.s_time = s_time;
    }

    public String getEn() {
        return en;
    }

    public void setEn(String en) {
        this.en = en;
    }

    public String getU_ud() {
        return u_ud;
    }

    public void setU_ud(String u_ud) {
        this.u_ud = u_ud;
    }

    public String getU_mid() {
        return u_mid;
    }

    public void setU_mid(String u_mid) {
        this.u_mid = u_mid;
    }

    public String getU_sd() {
        return u_sd;
    }

    public void setU_sd(String u_sd) {
        this.u_sd = u_sd;
    }

    public String getC_time() {
        return c_time;
    }

    public void setC_time(String c_time) {
        this.c_time = c_time;
    }

    public String getL() {
        return l;
    }

    public void setL(String l) {
        this.l = l;
    }

    public String getB_iev() {
        return b_iev;
    }

    public void setB_iev(String b_iev) {
        this.b_iev = b_iev;
    }

    public String getB_rst() {
        return b_rst;
    }

    public void setB_rst(String b_rst) {
        this.b_rst = b_rst;
    }

    public String getP_url() {
        return p_url;
    }

    public void setP_url(String p_url) {
        this.p_url = p_url;
    }

    public String getP_ref() {
        return p_ref;
    }

    public void setP_ref(String p_ref) {
        this.p_ref = p_ref;
    }

    public String getTt() {
        return tt;
    }

    public void setTt(String tt) {
        this.tt = tt;
    }

    public String getPl() {
        return pl;
    }

    public void setPl(String pl) {
        this.pl = pl;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getOid() {
        return oid;
    }

    public void setOid(String oid) {
        this.oid = oid;
    }

    public String getOn() {
        return on;
    }

    public void setOn(String on) {
        this.on = on;
    }

    public String getCua() {
        return cua;
    }

    public void setCua(String cua) {
        this.cua = cua;
    }

    public String getCut() {
        return cut;
    }

    public void setCut(String cut) {
        this.cut = cut;
    }

    public String getPt() {
        return pt;
    }

    public void setPt(String pt) {
        this.pt = pt;
    }

    public String getCa() {
        return ca;
    }

    public void setCa(String ca) {
        this.ca = ca;
    }

    public String getAc() {
        return ac;
    }

    public void setAc(String ac) {
        this.ac = ac;
    }

    public String getKv_() {
        return kv_;
    }

    public void setKv_(String kv_) {
        this.kv_ = kv_;
    }

    public String getDu() {
        return du;
    }

    public void setDu(String du) {
        this.du = du;
    }

    public String getBrowserName() {
        return browserName;
    }

    public void setBrowserName(String browserName) {
        this.browserName = browserName;
    }

    public String getBrowserVersion() {
        return browserVersion;
    }

    public void setBrowserVersion(String browserVersion) {
        this.browserVersion = browserVersion;
    }

    public String getOsName() {
        return osName;
    }

    public void setOsName(String osName) {
        this.osName = osName;
    }

    public String getOsVersion() {
        return osVersion;
    }

    public void setOsVersion(String osVersion) {
        this.osVersion = osVersion;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.ver);
        out.writeUTF(this.s_time);
        out.writeUTF(this.en);
        out.writeUTF(this.u_ud);
        out.writeUTF(this.u_mid);
        out.writeUTF(this.u_sd);
        out.writeUTF(this.c_time);
        out.writeUTF(this.l);
        out.writeUTF(this.b_iev);
        out.writeUTF(this.b_rst);
        out.writeUTF(this.p_url);
        out.writeUTF(this.p_ref);
        out.writeUTF(this.tt);
        out.writeUTF(this.pl);
        out.writeUTF(this.ip);
        out.writeUTF(this.oid);
        out.writeUTF(this.on);
        out.writeUTF(this.cua);
        out.writeUTF(this.cut);
        out.writeUTF(this.pt);
        out.writeUTF(this.ca);
        out.writeUTF(this.ac);
        out.writeUTF(this.kv_);
        out.writeUTF(this.du);
        out.writeUTF(this.browserName);
        out.writeUTF(this.browserVersion);
        out.writeUTF(this.osName);
        out.writeUTF(this.osVersion);
        out.writeUTF(this.country);
        out.writeUTF(this.province);
        out.writeUTF(this.city);
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        this.ver=in.readUTF();
        this.s_time=in.readUTF();
        this.en = in.readUTF();
        this.u_ud = in.readUTF();
        this.u_mid = in.readUTF();
        this.u_sd = in.readUTF();
        this.c_time = in.readUTF();
        this.l = in.readUTF();
        this.b_iev = in.readUTF();
        this.b_rst = in.readUTF();
        this.p_url = in.readUTF();
        this.p_ref = in.readUTF();
        this.tt = in.readUTF();
        this.pl = in.readUTF();
        this.ip = in.readUTF();
        this.oid = in.readUTF();
        this.on = in.readUTF();
        this.cua = in.readUTF();
        this.cut = in.readUTF();
        this.pt = in.readUTF();
        this.ca = in.readUTF();
        this.ac = in.readUTF();
        this.kv_ = in.readUTF();
        this.du = in.readUTF();
        this.browserName = in.readUTF();
        this.browserVersion = in.readUTF();
        this.osName = in.readUTF();
        this.osVersion = in.readUTF();
        this.country = in.readUTF();
        this.province = in.readUTF();
        this.city = in.readUTF();
        

    }
    
}
