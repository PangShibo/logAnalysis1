package com.phone.analysis.mr.model.base;

import com.phone.common.DateEnum;
import com.phone.util.TimeUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.Objects;

public class DateDimension extends BaseDimension {
    

    private int id;
    private int year;
    private int season;
    private int month;
    private int week;
    private int day;
    private Date calendar = new Date();
    private String type;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DateDimension that = (DateDimension) o;
        return id == that.id &&
                year == that.year &&
                season == that.season &&
                month == that.month &&
                week == that.week &&
                day == that.day &&
                Objects.equals(calendar, that.calendar) &&
                Objects.equals(type, that.type);
    }
    @Override
    public String toString() {
        return "DateDimension{" +
                "id=" + id +
                ", year=" + year +
                ", season=" + season +
                ", month=" + month +
                ", week=" + week +
                ", day=" + day +
                ", calendar=" + calendar +
                ", type='" + type + '\'' +
                '}';
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, year, season, month, week, day, calendar, type);
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getSeason() {
        return season;
    }

    public void setSeason(int season) {
        this.season = season;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public int getWeek() {
        return week;
    }

    public void setWeek(int week) {
        this.week = week;
    }

    public int getDay() {
        return day;
    }

    public void setDay(int day) {
        this.day = day;
    }

    public Date getCalendar() {
        return calendar;
    }

    public void setCalendar(Date calendar) {
        this.calendar = calendar;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
    public DateDimension(){
        
    }
    public DateDimension(int id,int year,int season,int month,int week,int day,Date calendar,String type){
        this.id=id;
        this.year=year;
        this.season=season;
        this.month=month;
        this.week=week;
        this.day=day;
        this.calendar=calendar;
        this.type = type;


    }
    public DateDimension(int year,int season,int month,int week,int day,Date calendar,String type){
        this.year=year;
        this.season=season;
        this.month=month;
        this.week=week;
        this.day=day;
        this.calendar=calendar;
        this.type = type;

    }

    @Override
    public int compareTo(BaseDimension o) {
        if (this == o){
            return 0;
        }
        DateDimension other = (DateDimension)o;
        int tmp = this.id - other.id;
        if (tmp != 0){
            return tmp;
        }
        tmp = this.year - other.year;
        if (tmp != 0){
            return tmp;
        }
        tmp = this.season - other.season;
        if (tmp != 0){
            return tmp;
        }
        tmp = this.month - other.month;
        if (tmp != 0){
            return tmp;
        }
        tmp = this.week - other.week;
        if (tmp != 0){
            return tmp;
        }
        tmp = this.day - other.day;
        if (tmp != 0){
            return tmp;
        }
        return this.type.compareTo(other.type);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.id);
        out.writeInt(this.year);
        out.writeInt(this.season);
        out.writeInt(this.month);
        out.writeInt(this.week);
        out.writeInt(this.day);
        out.writeLong(this.calendar.getTime());
        out.writeUTF(this.type);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readInt();
        this.year = in.readInt();
        this.season = in.readInt();
        this.month = in.readInt();
        this.week = in.readInt();
        this.day = in.readInt();
        this.calendar.setTime(in.readLong());
        this.type = in.readUTF();

    }

    /**
     * 构建时间维度对象
     * @param time  时间戳
     * @param type 枚举类型，根据传入的时间维度的不同构建不同的维度对象
     * @return
     */
    public static DateDimension buildDate(long time, DateEnum type){
        //构建年类型的时间维度对象
        //年类型 指的是该年的第一天
        int year = TimeUtil.getDateInfo(time,DateEnum.YEAR);
        Calendar calendar = Calendar.getInstance();
        if (type.equals(DateEnum.YEAR)){
            calendar.setTimeInMillis(time);
            return new DateDimension(year,0,0,0,1,calendar.getTime(),type.dateType);
        }
        //构建季度类型 时间维度对象
        int season = TimeUtil.getDateInfo(time,DateEnum.SEASON);
        if (type.equals(DateEnum.SEASON)){
            /**
             * 1 1
             * 2 4
             * 3 7
             * 4 10
             */
            int month = season*3 - 2;
            return new DateDimension(year,season,month,0,1,calendar.getTime(),type.dateType);
        }
        //构建月类型的维度对象
        //月类型 赋值的是该月的第一天
        int month = TimeUtil.getDateInfo(time,DateEnum.MONTH);
        if (type.equals(DateEnum.MONTH)){
            calendar.set(year,month-1,1);
            return new DateDimension(year,season,month,0,1,calendar.getTime(),type.dateType);
        }

        //构建周类型的时间维度对象
        //周类型指的是该周的第一天
        int week = TimeUtil.getDateInfo(time,DateEnum.WEEK);
        if (type.equals(DateEnum.WEEK)){
            //获取该周的第一天
            long firstDayOfweek = TimeUtil.getFirstDayOfWeek(time);
            year = TimeUtil.getDateInfo(firstDayOfweek,DateEnum.YEAR);
            season = TimeUtil.getDateInfo(firstDayOfweek,DateEnum.SEASON);
            month = TimeUtil.getDateInfo(firstDayOfweek,DateEnum.MONTH);
            int day = TimeUtil.getDateInfo(firstDayOfweek,DateEnum.DAY);
            calendar.set(year,month-1,day);
            return new DateDimension(year,season,month,week,day,calendar.getTime(),type.dateType);
        }
        //构建天类型的时间维度对象
        int day = TimeUtil.getDateInfo(time,DateEnum.DAY);
        if (type.equals(DateEnum.DAY)){
            calendar.set(year,month-1,day);
            return new DateDimension(year,season,month,week,day,calendar.getTime(),type.dateType);
        }
        throw new RuntimeException("该日期类型不支持获取对应的时间维度对象,dateType"+type.dateType);
    }

}