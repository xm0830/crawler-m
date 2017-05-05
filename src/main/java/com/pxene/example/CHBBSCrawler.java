package com.pxene.example;

import com.pxene.api.PageExtract;
import com.pxene.api.PipeLine;
import com.pxene.fetcher.FetchResult;
import com.pxene.fetcher.Processor;
import com.pxene.scheduler.TimeRangeRule;
import com.pxene.scheduler.TimeRangeSheduler;
import com.pxene.storage.HBaseStorage;
import com.pxene.storage.Storage;

import java.io.IOException;
import java.util.List;

/**
 * Created by xuming on 2017/5/3.
 */
public class CHBBSCrawler {
    public static void main(String[] args) throws IOException {
        PageExtract categoryPage = PageExtract.builder().httpSender()
                .addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
                .addHeader("Accept-Encoding", "gzip, deflate, sdch")
                .addHeader("Accept-Language", "zh-CN,zh;q=0.8")
                .addHeader("Cache-Control", "max-age=0")
                .addHeader("Cookie", "fvlid=1493109044271mQd2SHqO; sessionip=36.110.73.210; sessionid=6C536D37-02C1-463A-B5FF-C76E17C4AFAC%7C%7C2017-04-25+16%3A30%3A50.191%7C%7Cwww.baidu.com; ahpau=1; bdshare_firstime=1493109090091; guidance=true; mallsfvi=1493367299924jBMxRTcd%7Cwww.autohome.com.cn%7C103414; sessionuid=6C536D37-02C1-463A-B5FF-C76E17C4AFAC%7C%7C2017-04-25+16%3A30%3A50.191%7C%7Cwww.baidu.com; __utma=1.1033022094.1493109047.1493379093.1493690212.4; __utmc=1; __utmz=1.1493690212.4.3.utmcsr=sogou|utmccn=(organic)|utmcmd=organic|utmctr=%E6%B1%BD%E8%BD%A6%E4%B9%8B%E5%AE%B6; ASP.NET_SessionId=qjuhyusfscqswxov3r2kgo42; ahpvno=59; ref=www.sogou.com%7C%E6%B1%BD%E8%BD%A6%E4%B9%8B%E5%AE%B6%7C0%7Cwww.baidu.com%7C2017-05-02+12%3A51%3A39.315%7C2017-05-02+09%3A55%3A56.690; sessionvid=66F204C9-3E8E-44AE-8FD8-8A08F8D19098; area=110199")
                .addHeader("Upgrade-Insecure-Requests", "1").end().htmlFetcher()
                .configMultiValueColumn("url", "div.findcont-choose > a", "href")
                .configValueProcessor("url", new Processor() {
                    @Override
                    public String process(String value, String url) {
                        return "http://k.autohome.com.cn" + value;
                    }
                }).end()
                .build();

        PageExtract carPage = PageExtract.builder().httpSender()
                .addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
                .addHeader("Accept-Encoding", "gzip, deflate, sdch")
                .addHeader("Accept-Language", "zh-CN,zh;q=0.8")
                .addHeader("Cookie", "fvlid=1493109044271mQd2SHqO; sessionip=36.110.73.210; sessionid=6C536D37-02C1-463A-B5FF-C76E17C4AFAC%7C%7C2017-04-25+16%3A30%3A50.191%7C%7Cwww.baidu.com; ahpau=1; bdshare_firstime=1493109090091; guidance=true; mallsfvi=1493367299924jBMxRTcd%7Cwww.autohome.com.cn%7C103414; sessionuid=6C536D37-02C1-463A-B5FF-C76E17C4AFAC%7C%7C2017-04-25+16%3A30%3A50.191%7C%7Cwww.baidu.com; __utma=1.1033022094.1493109047.1493379093.1493690212.4; __utmc=1; __utmz=1.1493690212.4.3.utmcsr=sogou|utmccn=(organic)|utmcmd=organic|utmctr=%E6%B1%BD%E8%BD%A6%E4%B9%8B%E5%AE%B6; ASP.NET_SessionId=qjuhyusfscqswxov3r2kgo42; ahpvno=68; ref=www.sogou.com%7C%E6%B1%BD%E8%BD%A6%E4%B9%8B%E5%AE%B6%7C0%7Cwww.baidu.com%7C2017-05-02+13%3A05%3A35.454%7C2017-05-02+09%3A55%3A56.690; sessionvid=66F204C9-3E8E-44AE-8FD8-8A08F8D19098; area=110199; ahrlid=1493701586744DU84PETr-1493701710011")
                .addHeader("Upgrade-Insecure-Requests", "1").end().htmlFetcher()
                .configMultiValueColumn("url", "ul.list-cont > li > div.cont-name > a", "href")
                .configValueProcessor("url", new Processor() {
                    @Override
                    public String process(String value, String url) {
                        return String.format("http://club.autohome.com.cn/bbs/forum-c-%s-", value.replaceAll("/", ""));
                    }
                }).end()
                .build();

        PageExtract listPage = PageExtract.builder().httpSender()
                .addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
                .addHeader("Accept-Encoding", "gzip, deflate, sdch")
                .addHeader("Accept-Language", "zh-CN,zh;q=0.8")
                .addHeader("Cache-Control", "max-age=0")
                .addHeader("Cookie", "fvlid=1493109044271mQd2SHqO; sessionip=36.110.73.210; sessionid=6C536D37-02C1-463A-B5FF-C76E17C4AFAC%7C%7C2017-04-25+16%3A30%3A50.191%7C%7Cwww.baidu.com; ahpau=1; mallsfvi=1493367299924jBMxRTcd%7Cwww.autohome.com.cn%7C103414; sessionuid=6C536D37-02C1-463A-B5FF-C76E17C4AFAC%7C%7C2017-04-25+16%3A30%3A50.191%7C%7Cwww.baidu.com; __utma=1.1033022094.1493109047.1493786008.1493805059.7; __utmb=1.0.10.1493805059; __utmc=1; __utmz=1.1493805059.7.5.utmcsr=baidu|utmccn=(organic)|utmcmd=organic; historybbsName4=o-210763%7C%E6%9C%AC%E7%94%B0%E6%91%A9%E6%89%98%E8%BD%A6%2Cc-2123%7C%E5%93%88%E5%BC%97H6%2Cc-3195%7C%E7%91%9E%E8%99%8E5; ahpvno=132; ref=www.baidu.com%7C0%7C0%7Cwww.sogou.com%7C2017-05-03+17%3A59%3A07.833%7C2017-05-03+12%3A32%3A34.347; sessionvid=F7A6ABBE-1E46-490C-AAC6-A4805E644A29; area=110199; ahrlid=1493805600186vaiJvwPD-1493806149176")
                .addHeader("Upgrade-Insecure-Requests", "1").end().htmlFetcher()
                .configMultiValueColumn("url", "#subcontent > dl > dt > a.a_topic", "href")
                .configValueProcessor("url", new Processor() {
                    @Override
                    public String process(String value, String url) {
                        return "http://club.autohome.com.cn" + value;
                    }
                }).end()
                .build();

        PageExtract resultPage = PageExtract.builder().httpSender()
                .addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
                .addHeader("Accept-Encoding", "gzip, deflate, sdch")
                .addHeader("Accept-Language", "zh-CN,zh;q=0.8")
                .addHeader("Cache-Control", "max-age=0")
                .addHeader("Cookie", "fvlid=1493109044271mQd2SHqO; sessionip=36.110.73.210; sessionid=6C536D37-02C1-463A-B5FF-C76E17C4AFAC%7C%7C2017-04-25+16%3A30%3A50.191%7C%7Cwww.baidu.com; ahpau=1; mallsfvi=1493367299924jBMxRTcd%7Cwww.autohome.com.cn%7C103414; sessionuid=6C536D37-02C1-463A-B5FF-C76E17C4AFAC%7C%7C2017-04-25+16%3A30%3A50.191%7C%7Cwww.baidu.com; __utma=1.1033022094.1493109047.1493786008.1493805059.7; __utmb=1.0.10.1493805059; __utmc=1; __utmz=1.1493805059.7.5.utmcsr=baidu|utmccn=(organic)|utmcmd=organic; historybbsName4=o-210763%7C%E6%9C%AC%E7%94%B0%E6%91%A9%E6%89%98%E8%BD%A6%2Cc-2123%7C%E5%93%88%E5%BC%97H6%2Cc-3195%7C%E7%91%9E%E8%99%8E5; ahpvno=134; ref=www.baidu.com%7C0%7C0%7Cwww.sogou.com%7C2017-05-03+18%3A11%3A10.520%7C2017-05-03+12%3A32%3A34.347; sessionvid=F7A6ABBE-1E46-490C-AAC6-A4805E644A29; area=110199")
                .addHeader("Upgrade-Insecure-Requests", "1").end().htmlFetcher()
                .configSingleValueColumn("title", "#F0 > div.conright > div.rconten > h1 > div", null)
                .configSingleValueColumn("content", "#F0 > div.conright > div.rconten > div.conttxt", null)
                .configSingleValueColumn("bbs", "#consnav > span:nth-child(2) > a", null).end()
                .enbalePageUrlOutput("url")
                .addConstColumnOutput("source", "汽车之家")
                .build();


        PipeLine pipeLine = new PipeLine(new TimeRangeSheduler(), new TimeRangeRule("0700", "2300"));

        String rootUrl = "http://k.autohome.com.cn/suva1/";
        String rootRefer = "http://k.autohome.com.cn/";
        FetchResult result = pipeLine.pipe(rootUrl, rootRefer, categoryPage, carPage);
        PipeLine.close(categoryPage, carPage);

        Storage storage = new HBaseStorage("xuming.car_bbs");

        List<String> urls = result.select("url");
        for (String url : urls) {
            String tplUrl = url + "%s";
            String tplRefer = url + "%s";
            for (int i = 1; ; i++) {
                String realUrl = String.format(tplUrl, "1.html");
                String realRefer = null;
                if (i == 1) {
                    realRefer = String.format(tplRefer, "1.html");
                } else {
                    realRefer = String.format(tplRefer, (i-1) + ".html");
                }

                long count = pipeLine.pipe(realUrl, realRefer, storage, listPage, resultPage);
                if (count <= 0) {
                    break;
                }
            }
        }

        PipeLine.close(storage, listPage, resultPage);
    }
}
