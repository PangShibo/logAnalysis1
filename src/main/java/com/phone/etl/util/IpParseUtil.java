package com.phone.etl.util;

import com.alibaba.fastjson.JSONObject;
import com.phone.common.GlobalConstants;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;


/**
 * 解析ip，解析成ip地域信息
 */

public class IpParseUtil extends IPSeeker{
    private static Logger logger=Logger.getLogger(IpParseUtil.class);
    //纯真数据库解析
    public static RegionInfo ipParse(String ip){
        RegionInfo regionInfo = new RegionInfo();
        if(StringUtils.isNotEmpty(ip)){
            String country = IPSeeker.getInstance().getCountry(ip);
            if(StringUtils.isNotEmpty(country.trim())){
                if(country.equals("局域网")){
                    regionInfo.setCountry("中国");
                    regionInfo.setProvince("北京市");
                    regionInfo.setCity("昌平区");
                }else {
                    int index = country.indexOf("省");
                    if(index>0){
                        regionInfo.setCountry("中国");
                        regionInfo.setProvince(country.substring(0,index+1));
                        int  index2=country.indexOf("市");
                        if(index2>0){
                            regionInfo.setCity(country.substring(index+1,index2+1));
                        }
                    }else {
                        String flag = country.substring(0,2);
                        switch (flag){
                            case "内蒙":
                                regionInfo.setProvince("内蒙古");
                                country.substring(3);
                                index=country.indexOf("市");
                                if(index>0){
                                    regionInfo.setCity(country.substring(0,index+1));
                                }
                                break;
                            case "宁夏":
                            case "广西":
                            case "新疆":
                            case "西藏":
                                regionInfo.setProvince(flag+"省");
                                country.substring(2);
                                index=country.indexOf("市");
                                if(index>0){
                                    regionInfo.setCity(country.substring(0,index+1));
                                }
                                break;
                            case "北京":
                            case "天津":
                            case "上海":
                            case "重庆":
                                regionInfo.setProvince(flag + "市");
                                country.substring(2);
                                index = country.indexOf("区");
                                if (index > 0) {
                                    char ch = country.charAt(index - 1);
                                    if (ch != '小' || ch != '校' || ch != '军') {
                                        regionInfo.setCity(country.substring(0, index + 1));
                                    }
                                }

                                index = country.indexOf("县");
                                if (index > 0) {
                                    regionInfo.setCity(country.substring(0, index + 1));
                                }
                                break;
                            case "香港":
                            case "澳门":
                            case "台湾":
                                regionInfo.setProvince(flag + "特别行政区");
                                break;
                            default:
                                break;

                        }
                    }
                }
            }
        }
        return regionInfo;
    }

    /**
     * 淘宝解析IP
     * @param url http请求的url
     * @param charset 返回字符串的编码
     * @return
     */
    public static RegionInfo IpParse2(String url,String charset){
        RegionInfo regionInfo = new RegionInfo();
        try {
            if (StringUtils.isEmpty(url) || !url.startsWith("http")){
                throw new RuntimeException("url格式错误");
            }
            //代码走到这里
            //获取http请求的客户端
            HttpClient client = new HttpClient();
            //获取发送get请求的对象
            GetMethod method = new GetMethod(url);
            if(null!=charset){
                method.addRequestHeader("Content-Type", "application/x-www-form-urlencoded; charset=" + charset);
            }else {
                method.addRequestHeader("Content-Type", "application/x-www-form-urlencoded; charset=" + "utf-8");
            }
            //执行get请求,返回状态码，
            int statusCode = client.executeMethod(method);
            //验证状态码
            if (statusCode!= HttpStatus.SC_OK){
                System.out.println("failed message:"+method.getStatusLine());
                return regionInfo;
            }
            //代码走到这里，状态正常的
            //获取返回的字符串，转成字节数组
            byte []responseBody =  method.getResponseBodyAsString().getBytes(charset);
            //重新编码，返回字符串
            String response = new String(responseBody,"utf-8");
            //将返回的字符串转换成json对象
            JSONObject jo = JSONObject.parseObject(response);
            //根据data获取json对象
            JSONObject jo1 = JSONObject.parseObject(jo.getString("data"));
            //设置国家、省、市
            regionInfo.setCountry(jo1.getString("country"));
            regionInfo.setProvince(jo1.getString("region"));
            regionInfo.setCity(jo1.getString("city"));
        } catch (Exception e) {
            logger.error("解析IP异常",e);
        }
        return regionInfo;
    }

    /**
     * 用于封装解析出来的地域信息
     */
    public static class RegionInfo{
        private String DEFAULT_VALUE = GlobalConstants.DEFAULT_VALUE;
        private String country = DEFAULT_VALUE;
        private String province = DEFAULT_VALUE;
        private String city = DEFAULT_VALUE;

        public RegionInfo() {
        }

        public RegionInfo(String country, String province, String city) {
            this.country = country;
            this.province = province;
            this.city = city;
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
        public String toString() {
            return "RegionInfo{" +
                    "country='" + country + '\'' +
                    ", province='" + province + '\'' +
                    ", city='" + city + '\'' +
                    '}';
        }
    }

}