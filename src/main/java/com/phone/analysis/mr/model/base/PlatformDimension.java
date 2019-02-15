package com.phone.analysis.mr.model.base;

import com.phone.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class PlatformDimension extends BaseDimension {

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PlatformDimension that = (PlatformDimension) o;
        return id == that.id &&
                Objects.equals(platformName, that.platformName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, platformName);
    }

    private int id;
    private String platformName;

    @Override
    public String toString() {
        return "PlatformDimension{" +
                "id=" + id +
                ", platformName='" + platformName + '\'' +
                '}';
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getPlatformName() {
        return platformName;
    }

    public void setPlatformName(String platformName) {
        this.platformName = platformName;
    }
    public PlatformDimension(){
        
    }

    public PlatformDimension(int id, String platformName) {
        this.id=id;
        this.platformName=platformName;
        
    }
    public PlatformDimension(String platformName){
        this.platformName=platformName;
        
}

   
    
    
    
    @Override
    public int compareTo(BaseDimension o) {
        if (this==o){
            return 0;
        }
        PlatformDimension other= (PlatformDimension) o;
        int tmp=this.id-other.id;
        if (tmp!=0){
            return tmp;
        }
        tmp=this.platformName.compareTo(other.platformName);
        if (tmp!=0){
            return tmp;
        }
        
        return this.platformName.compareTo(other.platformName);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(id);
        out.writeUTF(platformName);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readInt();
        this.platformName=in.readUTF();


    }

    /**
     * 获取一个实例对象，如果平台名称未获取到，赋值一个默认值
     * @param platformName
     * @return
     */
    public static PlatformDimension getInstance(String platformName){
        String pl = StringUtils.isEmpty(platformName)? GlobalConstants.DEFAULT_VALUE:platformName;
        return new PlatformDimension(pl);
    }
}
