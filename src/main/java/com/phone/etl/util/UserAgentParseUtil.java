package com.phone.etl.util;

import cz.mallat.uasparser.OnlineUpdater;
import cz.mallat.uasparser.UASparser;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * 浏览器解析工具
 */

public class UserAgentParseUtil {
    private static Logger logger = Logger.getLogger(UserAgentParseUtil.class);

    private static UASparser uaSparser = null;

    static {
        try {
            uaSparser = new UASparser(OnlineUpdater.getVendoredInputStream());
        } catch (IOException e) {
            logger.error("获取解析浏览器对象失败", e);
        }
    }

    public static AgentInfo userAgentParse(String userAgent) {
        AgentInfo info = null;
        try {
            if (userAgent != null) {
                info = new AgentInfo();
                info.setBrowserName(uaSparser.parse(userAgent).getUaFamily());
                info.setBrowserVersion(uaSparser.parse(userAgent).getBrowserVersionInfo());
                info.setOsName(uaSparser.parse(userAgent).getOsFamily());
                info.setOsVersion(uaSparser.parse(userAgent).getOsName());

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return info;
    }


    /**
     * 封装浏览器信息的内部类
     */
    public static class AgentInfo {
        private String browserName;
        private String osVersion;
        private String osName;

        public AgentInfo() {
        }

        public AgentInfo(String browserName, String browserVersion, String osName, String osVersion) {
            this.browserName = browserName;
            this.browserVersion = browserVersion;
            this.osName = osName;
            this.osVersion = osVersion;
        }

        public void setBrowserName(String browserName) {
            this.browserName = browserName;
        }

        public void setBrowserVersion(String browserVersion) {
            this.browserVersion = browserVersion;
        }

        public void setOsName(String osName) {
            this.osName = osName;
        }

        public void setOsVersion(String osVersion) {
            this.osVersion = osVersion;
        }

        private String browserVersion;


        public String getBrowserName() {
            return browserName;
        }

        public String getBrowserVersion() {
            return browserVersion;
        }

        public String getOsName() {
            return osName;
        }

        public String getOsVersion() {
            return osVersion;
        }

        @Override
        public String toString() {
            return "AgentInfo{" +
                    "browserName='" + browserName + '\'' +
                    ", osVersion='" + osVersion + '\'' +
                    ", osName='" + osName + '\'' +
                    ", browserVersion='" + browserVersion + '\'' +
                    '}';
        }


    }
}
