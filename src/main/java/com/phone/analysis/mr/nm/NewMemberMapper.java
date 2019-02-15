package com.phone.analysis.mr.nm;

import com.phone.analysis.mr.model.base.BrowserDimension;
import com.phone.analysis.mr.model.base.DateDimension;
import com.phone.analysis.mr.model.base.KpiDimension;
import com.phone.analysis.mr.model.base.PlatformDimension;
import com.phone.analysis.mr.model.key.StatsCommonDimension;
import com.phone.analysis.mr.model.key.StatsUserDimension;
import com.phone.analysis.mr.model.value.TimeOutputValue;
import com.phone.common.DateEnum;
import com.phone.common.GlobalConstants;
import com.phone.common.KpiType;
import com.phone.util.MemberUtil;
import com.phone.util.TimeUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @ClassName NewUserMapper
 * @Author lyd
 * @Date $ $
 * @Vesion 1.0
 * @Description 新增会员：截止今天为止第一次访问网站的会员id的去重个数。
 * */

public class NewMemberMapper extends Mapper<LongWritable,Text,StatsUserDimension, TimeOutputValue> {
    private static final Logger logger = Logger.getLogger(NewMemberMapper.class);
    private StatsUserDimension k = new StatsUserDimension();
    private TimeOutputValue v = new TimeOutputValue();
    //kpi
    private KpiDimension newMemberkpi = new KpiDimension(KpiType.NEW_MEMBER.kpiName);
    private KpiDimension browserNewMemberkpi = new KpiDimension(KpiType.BROWSER_NEW_MEMBER.kpiName);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //调用删除统计当天的新增会员,防止重跑某一天的作业  ... -d 2019-02-12  ---> 20190212
        //起始时间
        long start = System.currentTimeMillis();
        logger.info("使用redis删除当天所有会员数据。start:"+start);
        MemberUtil.deleteAllDataByRedis(context.getConfiguration().get(GlobalConstants.RUNNING_DATE).replaceAll("-",""));
        long end = System.currentTimeMillis();
        logger.info("使用redis删除当天所有会员数据。end:"+end+"。删除总耗时:"+(end-start));
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if(StringUtils.isEmpty(value.toString())){
            return;
        }
        //获取一行清洗后的数据
        String []  fields = value.toString().split("\u0001");
        //获取统计需要的数据
        String s_time = fields[31];
        String platform = fields[2];
        String memberId = fields[8];
        String browserName = fields[26];
        String browserVersion = fields[27];

        //判空
        if(StringUtils.isBlank(s_time) || s_time.equals("null") || StringUtils.isEmpty(memberId)){
            logger.warn("s_time&memberId must not null.s_time:"+s_time+" memberId:"+memberId);
            return;
        }

       //判断是否为新增会员，如果是新增会员继续输出到reduce，否则什么都不做
        long serverTime = Long.valueOf(s_time);
        String dt = TimeUtil.parseLong2String(serverTime,"yyyyMMdd");
        if(MemberUtil.isNewMemberByRedis(memberId,dt)){
            //为value赋值
            this.v.setId(memberId);
            //为key赋值
            DateDimension dateDimension = DateDimension.buildDate(serverTime, DateEnum.DAY);
            PlatformDimension platformDimension = new PlatformDimension(platform);
            BrowserDimension defaultBrowser = new BrowserDimension("","");

            StatsCommonDimension statsCommonDimension = this.k.getStatsCommonDimension();

            //为statsCommonDimension赋值
            statsCommonDimension.setDt(dateDimension);
            statsCommonDimension.setPl(platformDimension);
            statsCommonDimension.setKpi(newMemberkpi);

            this.k.setStatsCommonDimension(statsCommonDimension);
            this.k.setBrowserDimension(defaultBrowser);

            //输出
            context.write(this.k,this.v);

            //用于浏览器的
            statsCommonDimension.setKpi(browserNewMemberkpi);
            this.k.setBrowserDimension(new BrowserDimension(browserName,browserVersion));
            this.k.setStatsCommonDimension(statsCommonDimension);
            context.write(this.k,this.v);
        }
    }
}