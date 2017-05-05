package com.rainbow.example;

import com.rainbow.api.PageExtract;
import com.rainbow.api.PipeLine;
import com.rainbow.fetcher.FetchResult;
import com.rainbow.scheduler.TimeRangeRule;
import com.rainbow.scheduler.TimeRangeSheduler;
import com.rainbow.storage.HBaseStorage;
import com.rainbow.storage.Storage;

import java.io.IOException;

/**
 * Created by xuming on 2017/4/28.
 */
public class CHPingceCrawler {

    public static void main(String[] args) throws IOException {
        PageExtract countPage = PageExtract.builder().httpSender()
                .addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
                .addHeader("Accept-Encoding", "gzip, deflate, sdch")
                .addHeader("Accept-Language", "zh-CN,zh;q=0.8")
                .addHeader("Cache-Control", "max-age=0")
                .addHeader("Cookie", "ASP.NET_SessionId=wv45qzduxgbpymhw01xwidvt; fvlid=1493109044271mQd2SHqO; sessionip=36.110.73.210; sessionid=6C536D37-02C1-463A-B5FF-C76E17C4AFAC%7C%7C2017-04-25+16%3A30%3A50.191%7C%7Cwww.baidu.com; ahpau=1; mpvareaid=103414; mallsfvi=1493367299924jBMxRTcd%7Cwww.autohome.com.cn%7C103414; mallslvi=103414%7Cwww.autohome.com.cn%7C1493367299924jBMxRTcd; ahpvno=17; sessionuid=6C536D37-02C1-463A-B5FF-C76E17C4AFAC%7C%7C2017-04-25+16%3A30%3A50.191%7C%7Cwww.baidu.com; __utma=1.1033022094.1493109047.1493109047.1493367030.2; __utmb=1.0.10.1493367030; __utmc=1; __utmz=1.1493367030.2.2.utmcsr=baidu|utmccn=(organic)|utmcmd=organic; ref=www.baidu.com%7C0%7C0%7C0%7C2017-04-28+16%3A44%3A23.274%7C2017-04-25+16%3A30%3A50.191; sessionvid=791D896C-C1DF-4BE1-849E-50013AD3A0AF; area=110199")
                .addHeader("Upgrade-Insecure-Requests", "1").end()
                .htmlFetcher()
                .configSingleValueColumn("pageCount", "div.page > a:nth-last-child(2)", null).end()
                .build();

        String url = "http://www.autohome.com.cn/bestauto/";
        String refer = "http://www.autohome.com.cn/beijing/";
        FetchResult extract = new PipeLine().pipe(url, refer, countPage);
        PipeLine.close(countPage);
        int pageCount = Integer.parseInt(extract.select("pageCount").get(0));

        PageExtract listPage = PageExtract.builder().httpSender()
                .addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
                .addHeader("Accept-Encoding", "gzip, deflate, sdch")
                .addHeader("Accept-Language", "zh-CN,zh;q=0.8")
                .addHeader("Cache-Control", "max-age=0")
                .addHeader("Cookie", "ASP.NET_SessionId=wv45qzduxgbpymhw01xwidvt; fvlid=1493109044271mQd2SHqO; sessionip=36.110.73.210; sessionid=6C536D37-02C1-463A-B5FF-C76E17C4AFAC%7C%7C2017-04-25+16%3A30%3A50.191%7C%7Cwww.baidu.com; ahpau=1; mpvareaid=103414; mallsfvi=1493367299924jBMxRTcd%7Cwww.autohome.com.cn%7C103414; mallslvi=103414%7Cwww.autohome.com.cn%7C1493367299924jBMxRTcd; ahpvno=17; sessionuid=6C536D37-02C1-463A-B5FF-C76E17C4AFAC%7C%7C2017-04-25+16%3A30%3A50.191%7C%7Cwww.baidu.com; __utma=1.1033022094.1493109047.1493109047.1493367030.2; __utmb=1.0.10.1493367030; __utmc=1; __utmz=1.1493367030.2.2.utmcsr=baidu|utmccn=(organic)|utmcmd=organic; ref=www.baidu.com%7C0%7C0%7C0%7C2017-04-28+16%3A44%3A23.274%7C2017-04-25+16%3A30%3A50.191; sessionvid=791D896C-C1DF-4BE1-849E-50013AD3A0AF; area=110199")
                .addHeader("Upgrade-Insecure-Requests", "1").end().htmlFetcher()
                .configMultiValueColumn("url", "#bestautocontent > div div.tit > span:nth-child(1) > a", "href").end()
                .build();

        PageExtract detailPage = PageExtract.builder().httpSender()
                .addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
                .addHeader("Accept-Encoding", "gzip, deflate, sdch")
                .addHeader("Accept-Language", "zh-CN,zh;q=0.8")
                .addHeader("Cache-Control", "max-age=0")
                .addHeader("Cookie", "ASP.NET_SessionId=wv45qzduxgbpymhw01xwidvt; fvlid=1493109044271mQd2SHqO; sessionip=36.110.73.210; sessionid=6C536D37-02C1-463A-B5FF-C76E17C4AFAC%7C%7C2017-04-25+16%3A30%3A50.191%7C%7Cwww.baidu.com; ahpau=1; mpvareaid=103414; mallsfvi=1493367299924jBMxRTcd%7Cwww.autohome.com.cn%7C103414; mallslvi=103414%7Cwww.autohome.com.cn%7C1493367299924jBMxRTcd; sessionuid=6C536D37-02C1-463A-B5FF-C76E17C4AFAC%7C%7C2017-04-25+16%3A30%3A50.191%7C%7Cwww.baidu.com; __utma=1.1033022094.1493109047.1493109047.1493367030.2; __utmb=1.0.10.1493367030; __utmc=1; __utmz=1.1493367030.2.2.utmcsr=baidu|utmccn=(organic)|utmcmd=organic; ref=www.baidu.com%7C0%7C0%7C0%7C2017-04-28+16%3A55%3A47.791%7C2017-04-25+16%3A30%3A50.191; sessionvid=791D896C-C1DF-4BE1-849E-50013AD3A0AF; area=110199; ahpvno=21; ahrlid=14933707745720AsXy70S-1493370778213")
                .addHeader("Upgrade-Insecure-Requests", "1").end().htmlFetcher()
                .configSingleValueColumn("title", "#articlewrap > h1", null)
                .configSingleValueColumn("content", "#articleContent", null).end()
                .enbalePageUrlOutput("url")
                .addConstColumnOutput("source", "汽车之家")
                .build();

        Storage storage = new HBaseStorage("xuming.car_article");

        PipeLine pipeLine = new PipeLine(new TimeRangeSheduler(), new TimeRangeRule("0700", "2300"));
        String listUrl = "http://www.autohome.com.cn/bestauto/%s";
        String listReferUrl = "http://www.autohome.com.cn/bestauto/%s";
        for (int i = 1; i <= pageCount; i++) {
            String realListUrl = String.format(listUrl, i);
            String realListReferUrl = null;
            if (i == 1) {
                realListReferUrl = String.format(listReferUrl, "");
            } else {
                realListReferUrl = String.format(listReferUrl, i - 1);
            }

            pipeLine.pipe(realListUrl, realListReferUrl, storage, listPage, detailPage);
        }

        PipeLine.close(storage, countPage, listPage, detailPage);
    }
}
