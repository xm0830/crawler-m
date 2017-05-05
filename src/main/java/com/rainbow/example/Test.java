package com.rainbow.example;

import org.apache.commons.httpclient.URIException;

import java.net.URISyntaxException;

/**
 * Created by xuming on 2017/4/27.
 */
public class Test {

    public static void main(String[] args) throws URISyntaxException, URIException {
        String str = "Accept:text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8\n" +
                "Accept-Encoding:gzip, deflate, sdch\n" +
                "Accept-Language:zh-CN,zh;q=0.8\n" +
                "Cache-Control:max-age=0\n" +
                "Connection:keep-alive\n" +
                "Cookie:XCWEBLOG_testcookie=yes; authkey=d7f776d40b054649971435f4ddabbb10; cusloced=1; hmc_favcs=1994,3152,1909; hmc_favcar=119392,123298,120230; tracker_u=42_bdsempc; hmc_mt=1493977061860; XCWEBLOG_testcookie=yes; Hm_lvt_34b070f600a54baeebad2039a605f793=1493170116,1493262844,1493281753,1493977062; Hm_lpvt_34b070f600a54baeebad2039a605f793=1493977735; Hm_lvt_d877be9ebbae0bd16202a42d41e09f3e=1493170115,1493262843,1493281753,1493977062; Hm_lpvt_d877be9ebbae0bd16202a42d41e09f3e=1493977735; loc=901%7C%E7%9F%B3%E5%AE%B6%E5%BA%84\n" +
                "Host:news.huimaiche.com\n" +
                "Referer:http://news.huimaiche.com/\n" +
                "Upgrade-Insecure-Requests:1\n" +
                "User-Agent:Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36";

        String[] split = str.split("\n", -1);
        for (String line : split) {
            if (!line.contains("Host") && !line.contains("User-Agent") && !line.contains("Connection") && !line.contains("Referer")) {
                int index = line.indexOf(":");
                System.out.println(".addHeader(\"" + line.substring(0, index) + "\", \"" + line.substring(index+1) + "\")");
            }
        }

    }
}
