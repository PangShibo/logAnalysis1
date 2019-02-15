package com.phone.analysis.mr.nm;

import com.phone.analysis.mr.model.base.DateDimension;
import com.phone.analysis.mr.model.key.StatsBaseDimension;
import com.phone.analysis.mr.model.key.StatsUserDimension;
import com.phone.analysis.mr.model.value.OutputWritable;
import com.phone.analysis.mr.model.value.StatsOutputValue;
import com.phone.analysis.outputformat.IOutputValue;
import com.phone.analysis.service.IDimension;
import com.phone.analysis.service.impl.IDimensionImpl;
import com.phone.common.DateEnum;
import com.phone.common.GlobalConstants;
import com.phone.common.KpiType;
import com.phone.util.JdbcUtil;
import com.phone.util.TimeUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * 新增会员的赋值类
 */
public class NewMemberWritter implements IOutputValue {
    /**
     * 查询指定维度的昨天的总会员
     *
     * @param platformId
     * @param yesterdayId
     * @param browserId
     * @return
     */
    private int getYesterdayTotalUsers(int platformId, int yesterdayId, int browserId) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            //1、获取连接
            conn = JdbcUtil.getConn();
            //2、预处理sql
            ps = conn.prepareStatement("select `total_members` from `stats_device_browser` where `date_dimension_id` = ? and `platform_dimension_id` = ? and `browser_dimension_id` = ?");
            if (browserId == -1) {
                ps = conn.prepareStatement("select `total_members` from `stats_user` where `date_dimension_id` = ? and `platform_dimension_id` = ?");
            }
            //3、为占位符赋值
            ps.setInt(1, yesterdayId);
            ps.setInt(2, platformId);
            if (browserId != -1) {
                ps.setInt(3, browserId);
            }
            //4、查询操作
            rs = ps.executeQuery();
            //5、对结果集处理
            while (rs.next()) {
                return rs.getInt("total_members");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            JdbcUtil.close(conn, ps, rs);
        }
        return 0;
    }

    @Override
    public void output(StatsBaseDimension key, StatsOutputValue value, IDimension convert, PreparedStatement ps, Configuration conf) {
        StatsUserDimension statsUserDimension = (StatsUserDimension) key;
        OutputWritable mapWritableValue = (OutputWritable) value;
        int newMemberNums = ((IntWritable) mapWritableValue.getValue().get(new IntWritable(-1))).get();

        //为ps设置值
        int i = 0;
        int dataId = convert.getIDimensionByObject(statsUserDimension.getStatsCommonDimension().getDt());
        int platformId = convert.getIDimensionByObject(statsUserDimension.getStatsCommonDimension().getPl());
        //获取昨天的时间id
        DateDimension yesterdayDateDimension = DateDimension.buildDate(TimeUtil.parseString2Long(conf.get(GlobalConstants.RUNNING_DATE)) - GlobalConstants.DAY_OF_MILLSECOND, DateEnum.DAY);
        IDimension iDimensionConvert = new IDimensionImpl();
        int yesterdayId = iDimensionConvert.getIDimensionByObject(yesterdayDateDimension);
        //赋值
        try {
            ps.setInt(++i, dataId);

            ps.setInt(++i, platformId);
            int total_members = 0;
            if (value.getKpi().equals(KpiType.BROWSER_NEW_MEMBER)) {
                int browserId = convert.getIDimensionByObject(statsUserDimension.getBrowserDimension());
                ps.setInt(++i, browserId);
                total_members = getYesterdayTotalUsers(platformId, yesterdayId, browserId);
            } else {
                //当browserId=-1时代表是用户模块
                total_members = getYesterdayTotalUsers(platformId, yesterdayId, -1);
            }
            ps.setInt(++i, newMemberNums);
            //设置新增总用户
            ps.setInt(++i, total_members + newMemberNums);
            ps.setString(++i, conf.get(GlobalConstants.RUNNING_DATE));
            ps.setInt(++i, newMemberNums);
            ps.setInt(++i, total_members + newMemberNums);
            //将ps添加到batch
            ps.addBatch();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
