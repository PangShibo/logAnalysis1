
package com.phone.analysis.mr.au;

import com.phone.analysis.mr.model.base.BrowserDimension;
import com.phone.analysis.mr.model.base.DateDimension;
import com.phone.analysis.mr.model.base.KpiDimension;
import com.phone.analysis.mr.model.base.PlatformDimension;
import com.phone.analysis.mr.model.key.StatsCommonDimension;
import com.phone.analysis.mr.model.key.StatsUserDimension;
import com.phone.analysis.mr.model.value.TimeOutputValue;
import com.phone.common.DateEnum;
import com.phone.common.KpiType;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * 活跃用户：所有数据中，uuid的去重个数
 */
public class AcitveUserMapper extends Mapper<LongWritable,Text, StatsUserDimension, TimeOutputValue> {
    private static final Logger logger = Logger.getLogger(AcitveUserMapper.class);
    private StatsUserDimension k = new StatsUserDimension();
    private TimeOutputValue v = new TimeOutputValue();

    private KpiDimension activeUserKpi = new KpiDimension(KpiType.ACTIVE_USER.kpiName);
    private KpiDimension activeBrowserUserKpi = new KpiDimension(KpiType.BROWSER_ACTIVE_USER.kpiName);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if(StringUtils.isEmpty(line)){
            return ;
        }

        //拆分
        String[] fields = line.split("\u0001");
        //en是事件名称
        String en = fields[2];
            //获取想要的字段
            String serverTime = fields[1];
            String platform = fields[13];
            String uuid = fields[3];
            String browserName = fields[24];
            String browserVersion = fields[25];

            if(StringUtils.isEmpty(serverTime) || StringUtils.isEmpty(uuid)){
                logger.info("serverTime & uuid is null serverTime:"+serverTime+".uuid"+uuid);
                return;
            }

            //构造输出的key
            long stime = Long.valueOf(serverTime);
            this.v.setTime(stime);
            PlatformDimension platformDimension = PlatformDimension.getInstance(platform);
            DateDimension dateDimension = DateDimension.buildDate(stime, DateEnum.DAY);
            StatsCommonDimension statsCommonDimension = this.k.getStatsCommonDimension();
            //为StatsCommonDimension设值
            statsCommonDimension.setDt(dateDimension);
            statsCommonDimension.setPl(platformDimension);

            //用户模块新增用户
            //设置默认的浏览器对象(因为新增用户指标并不需要浏览器维度，所以赋值为空)
            BrowserDimension defaultBrowserDimension = new BrowserDimension("","");
            statsCommonDimension.setKpi(activeUserKpi);
            this.k.setBrowserDimension(defaultBrowserDimension);
            this.k.setStatsCommonDimension(statsCommonDimension);
            this.v.setId(uuid);
            context.write(this.k,this.v);//输出

            //浏览器模块新增用户
            statsCommonDimension.setKpi(activeBrowserUserKpi);
            BrowserDimension browserDimension = new BrowserDimension(browserName,browserVersion);
            this.k.setBrowserDimension(browserDimension);
            this.k.setStatsCommonDimension(statsCommonDimension);
            context.write(this.k,this.v);//输出
    }
}