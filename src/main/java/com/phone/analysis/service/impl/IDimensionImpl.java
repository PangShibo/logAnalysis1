package com.phone.analysis.service.impl;

import com.phone.analysis.mr.model.base.*;
import com.phone.analysis.service.IDimension;
import com.phone.util.JdbcUtil;

import java.sql.*;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @Description :获取维度ID的实现类
 * @Author cqh <caoqingghai@1000phone.com>
 * @Version V1.0
 * @Since 1.0
 * @Date 2019/1/5 10：31
 */
public class IDimensionImpl implements IDimension {
    //定义缓存，用来缓存维度ID
    //KEY：维度对象属性的拼接
    //VALUE：维度ID
    private Map<String,Integer> cache = new LinkedHashMap<String,Integer>(){
        @Override
        //移除最早的维度ID（按照添加的顺序移除的）
        protected boolean removeEldestEntry(Map.Entry eldest) {
            return this.size() > 3000;
        }
    };

    /**
     * 根据维度对象属性去获取维度ID
     * 首先从缓存中取
     * 缓存中没有查数据库
     * 数据库中没有插入并返回
     * @param baseDimension
     * @return
     */
    @Override
    public int getIDimensionByObject(BaseDimension baseDimension) {
        Connection conn = null;
        //构建缓存的key
        String cacheKey = buildCacheKey(baseDimension);
        //判断缓存的map中是否包含这个key
        if (this.cache.containsKey(cacheKey)){
            return this.cache.get(cacheKey);
        }
        //代码运行到这里说明缓存中没有
        //先到数据库中去查，如果查询的到，直接返回维度ID，添加到缓存中
        //如果数据库中查不到，插入，返回维度ID，添加到缓存中

        //构建查询和插入语句
        String sqls [] = null;
        if (baseDimension instanceof BrowserDimension){
            sqls = buildBrowserSqls(baseDimension);
        }else if (baseDimension instanceof KpiDimension){
            sqls = buildKpiSqls(baseDimension);
        }else if (baseDimension instanceof PlatformDimension){
            sqls = buildPlatSqls(baseDimension);
        } else if (baseDimension instanceof DateDimension){
            sqls = buildDateSqls(baseDimension);
        }
        //获取连接对象
        conn = JdbcUtil.getConn();
        int id = -1;
        synchronized (this){
            //执行sql语句
            id = excuteSql(conn,sqls,baseDimension);
        }
        //将维度ID存到缓存中
        cache.put(cacheKey,id);
        return id;
    }

    /**
     * 执行sql语句的方法
     * @param conn
     * @param sqls
     * @param baseDimension
     * @return
     */
    private int excuteSql(Connection conn, String[] sqls, BaseDimension baseDimension) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        //执行查询语句
        //首先获取查询语句
        try {
            String selectSql = sqls[1];
            ps = conn.prepareStatement(selectSql);
            //给语句赋值的方法
            setArgs(ps,baseDimension);
            //执行查询,返回一个结果集
            rs = ps.executeQuery();
            //判断查询结果是否有值
            if (rs.next()){
                return rs.getInt(1);
            }
            //如果代码运行到这里，说明数据库中不存在这个维度信息
            //插入数据库，并返回维度ID
            //获取插入语句，插入的同时可以返回主键
            ps = conn.prepareStatement(sqls[0],Statement.RETURN_GENERATED_KEYS);
            //为插入语句赋值
            setArgs(ps,baseDimension);
            //执行语句
            ps.executeUpdate();
            //获取插入后返回的结果集，结果集中存的这个主键，也就是我们的维度ID
            rs = ps.getGeneratedKeys();
            //判断rs是否有值
            if (rs.next()){
                return rs.getInt(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            JdbcUtil.close(conn,ps,rs);
        }
        return -1;
    }

    /**
     * sql语句赋值的方法
     * @param ps
     * @param baseDimension
     */
    private void setArgs(PreparedStatement ps, BaseDimension baseDimension) {
        try {
            int i = 0;
            if (baseDimension instanceof BrowserDimension){
                BrowserDimension browserDimension = (BrowserDimension)baseDimension;
                ps.setString(++i,browserDimension.getBrowserName());
                ps.setString(++i,browserDimension.getBrowserVersion());
            }else if (baseDimension instanceof KpiDimension){
                KpiDimension kpiDimension = (KpiDimension)baseDimension;
                ps.setString(++i,kpiDimension.getKpiName());

            }else if (baseDimension instanceof PlatformDimension){
                PlatformDimension platformDimension = (PlatformDimension)baseDimension;
                ps.setString(++i,platformDimension.getPlatformName());

            } else if (baseDimension instanceof DateDimension){
                DateDimension dateDimension = (DateDimension)baseDimension;
                ps.setInt(++i,dateDimension.getYear());
                ps.setInt(++i,dateDimension.getSeason());
                ps.setInt(++i,dateDimension.getMonth());
                ps.setInt(++i,dateDimension.getWeek());
                ps.setInt(++i,dateDimension.getDay());
                ps.setString(++i,dateDimension.getType());
                ps.setDate(++i,new Date(dateDimension.getCalendar().getTime()));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private String[] buildDateSqls(BaseDimension baseDimension) {
        String insertSql = "insert into dimension_date (year,season,month,week,day,type,calendar) values (?,?,?,?,?,?,?)";
        String selectSql = "select id from dimension_date where year = ? and season = ? and month = ? and week = ? and day = ? and type = ? and calendar = ? ";
        return new String[]{insertSql,selectSql};
    }

    private String[] buildPlatSqls(BaseDimension baseDimension) {
        String insertSql = "insert into dimension_platform (platform_name) values (?)";
        String selectSql = "select id from dimension_platform where platform_name = ?";
        return new String[]{insertSql,selectSql};
    }

    private String[] buildKpiSqls(BaseDimension baseDimension) {
        String insertSql = "insert into dimension_kpi (kpi_name) values (?)";
        String selectSql = "select id from dimension_kpi where kpi_name = ?";
        return new String[]{insertSql,selectSql};
    }

    private String[] buildBrowserSqls(BaseDimension baseDimension) {
        String insertSql = "insert into dimension_browser (browser_name,browser_version) values (?,?)";
        String selectSql = "select id from dimension_browser where browser_name = ? and browser_version = ? ";
        return new String[]{insertSql,selectSql};
    }

    /**
     * 构建缓存key的方法
     * @param baseDimension
     * @return
     */
    private String buildCacheKey(BaseDimension baseDimension) {
        StringBuffer sb = new StringBuffer();
        if (baseDimension instanceof BrowserDimension){
            sb.append("browser_");
            BrowserDimension browserDimension = (BrowserDimension)baseDimension;
            sb.append(browserDimension.getBrowserName());
            sb.append(browserDimension.getBrowserVersion());
        }else if (baseDimension instanceof KpiDimension){
            sb.append("kpi_");
            KpiDimension kpiDimension = (KpiDimension)baseDimension;
            sb.append(kpiDimension.getKpiName());
        }else if (baseDimension instanceof PlatformDimension){
            sb.append("platform_");
            PlatformDimension platformDimension = (PlatformDimension)baseDimension;
            sb.append(platformDimension.getPlatformName());
        } else if (baseDimension instanceof DateDimension){
            sb.append("date_");
            DateDimension dateDimension = (DateDimension)baseDimension;
            sb.append(dateDimension.getYear());
            sb.append(dateDimension.getSeason());
            sb.append(dateDimension.getMonth());
            sb.append(dateDimension.getWeek());
            sb.append(dateDimension.getDay());
            sb.append(dateDimension.getType());
    } else if (baseDimension instanceof EventDimension){
        sb.append("event_");
        DateDimension dateDimension = (DateDimension)baseDimension;
        sb.append(dateDimension.getYear());
        sb.append(dateDimension.getSeason());
        sb.append(dateDimension.getMonth());
        sb.append(dateDimension.getWeek());
        sb.append(dateDimension.getDay());
        sb.append(dateDimension.getType());
    }
        return sb !=null ?sb.toString() : null;
    }
}