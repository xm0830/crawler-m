package com.rainbow.example;

import com.rainbow.api.PageExtract;
import com.rainbow.api.PipeLine;
import com.rainbow.fetcher.FetchResult;
import com.rainbow.fetcher.Processor;
import com.rainbow.scheduler.TimeRangeRule;
import com.rainbow.scheduler.TimeRangeSheduler;
import com.rainbow.storage.HBaseStorage;
import com.rainbow.storage.Storage;

import java.io.IOException;

/**
 * Created by xuming on 2017/4/28.
 */
public class CHShuokeCrawler {

    public static void main(String[] args) throws IOException {
        PageExtract countPage = PageExtract.builder().httpSender()
                .addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
                .addHeader("Accept-Encoding", "gzip, deflate, sdch")
                .addHeader("Accept-Language", "zh-CN,zh;q=0.8")
                .addHeader("Cache-Control", "max-age=0")
                .addHeader("Cookie", "fvlid=1493109044271mQd2SHqO; sessionip=36.110.73.210; sessionid=6C536D37-02C1-463A-B5FF-C76E17C4AFAC%7C%7C2017-04-25+16%3A30%3A50.191%7C%7Cwww.baidu.com; ahpau=1; mallsfvi=1493367299924jBMxRTcd%7Cwww.autohome.com.cn%7C103414; sfvi=1493367517955FXKk7YZZ%7Chttp%3A%2F%2Fwww.autohome.com.cn%2Fbeijing%2F; sessionuid=6C536D37-02C1-463A-B5FF-C76E17C4AFAC%7C%7C2017-04-25+16%3A30%3A50.191%7C%7Cwww.baidu.com; __utma=1.1033022094.1493109047.1493379093.1493690212.4; __utmb=1.0.10.1493690212; __utmc=1; __utmz=1.1493690212.4.3.utmcsr=sogou|utmccn=(organic)|utmcmd=organic|utmctr=%E6%B1%BD%E8%BD%A6%E4%B9%8B%E5%AE%B6; ahpvno=8; slvi=103410%7Chttp%3A%2F%2Fwww.autohome.com.cn%2Fbeijing%2F; ref=www.sogou.com%7C%E6%B1%BD%E8%BD%A6%E4%B9%8B%E5%AE%B6%7C0%7Cwww.baidu.com%7C2017-05-02+10%3A01%3A24.030%7C2017-05-02+09%3A55%3A56.690; sessionvid=9EC1830B-732B-4FB4-A5E1-17EA664B90EB; area=110199")
                .addHeader("Upgrade-Insecure-Requests", "1").end()
                .htmlFetcher()
                .configSingleValueColumn("pageCount", "div.page > a:nth-last-child(2)", null).end()
                .build();

        String url = "http://shuoke.autohome.com.cn/";
        String refer = "http://www.autohome.com.cn/beijing/";
        FetchResult extract = new PipeLine().pipe(url, refer, countPage);
        PipeLine.close(countPage);
        int pageCount = Integer.parseInt(extract.select("pageCount").get(0));

        PageExtract listPage = PageExtract.builder().httpSender()
                .addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
                .addHeader("Accept-Encoding", "gzip, deflate, sdch")
                .addHeader("Accept-Language", "zh-CN,zh;q=0.8")
                .addHeader("Cache-Control", "max-age=0")
                .addHeader("Cookie", "fvlid=1493109044271mQd2SHqO; sessionip=36.110.73.210; sessionid=6C536D37-02C1-463A-B5FF-C76E17C4AFAC%7C%7C2017-04-25+16%3A30%3A50.191%7C%7Cwww.baidu.com; ahpau=1; mallsfvi=1493367299924jBMxRTcd%7Cwww.autohome.com.cn%7C103414; sfvi=1493367517955FXKk7YZZ%7Chttp%3A%2F%2Fwww.autohome.com.cn%2Fbeijing%2F; sessionuid=6C536D37-02C1-463A-B5FF-C76E17C4AFAC%7C%7C2017-04-25+16%3A30%3A50.191%7C%7Cwww.baidu.com; __utma=1.1033022094.1493109047.1493379093.1493690212.4; __utmb=1.0.10.1493690212; __utmc=1; __utmz=1.1493690212.4.3.utmcsr=sogou|utmccn=(organic)|utmcmd=organic|utmctr=%E6%B1%BD%E8%BD%A6%E4%B9%8B%E5%AE%B6; ahpvno=8; slvi=103410%7Chttp%3A%2F%2Fwww.autohome.com.cn%2Fbeijing%2F; ref=www.sogou.com%7C%E6%B1%BD%E8%BD%A6%E4%B9%8B%E5%AE%B6%7C0%7Cwww.baidu.com%7C2017-05-02+10%3A01%3A24.030%7C2017-05-02+09%3A55%3A56.690; sessionvid=9EC1830B-732B-4FB4-A5E1-17EA664B90EB; area=110199")
                .addHeader("Upgrade-Insecure-Requests", "1").end().htmlFetcher()
                .configMultiValueColumn("url", "#articleList > li > h4 > a", "href")
                .configValueProcessor("url", new Processor() {
                    @Override
                    public String process(String value, String url) {
                        return "http://shuoke.autohome.com.cn" + value;
                    }
                }).end()
                .build();

        PageExtract detailPage = PageExtract.builder().httpSender()
                .addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
                .addHeader("Accept-Encoding", "gzip, deflate, sdch")
                .addHeader("Accept-Language", "zh-CN,zh;q=0.8")
                .addHeader("Cache-Control", "max-age=0")
                .addHeader("Cookie", "fvlid=1493109044271mQd2SHqO; sessionip=36.110.73.210; sessionid=6C536D37-02C1-463A-B5FF-C76E17C4AFAC%7C%7C2017-04-25+16%3A30%3A50.191%7C%7Cwww.baidu.com; ahpau=1; mallsfvi=1493367299924jBMxRTcd%7Cwww.autohome.com.cn%7C103414; sfvi=1493367517955FXKk7YZZ%7Chttp%3A%2F%2Fwww.autohome.com.cn%2Fbeijing%2F; sessionuid=6C536D37-02C1-463A-B5FF-C76E17C4AFAC%7C%7C2017-04-25+16%3A30%3A50.191%7C%7Cwww.baidu.com; __utma=1.1033022094.1493109047.1493379093.1493690212.4; __utmb=1.0.10.1493690212; __utmc=1; __utmz=1.1493690212.4.3.utmcsr=sogou|utmccn=(organic)|utmcmd=organic|utmctr=%E6%B1%BD%E8%BD%A6%E4%B9%8B%E5%AE%B6; slvi=103410%7Chttp%3A%2F%2Fwww.autohome.com.cn%2Fbeijing%2F; ref=www.sogou.com%7C%E6%B1%BD%E8%BD%A6%E4%B9%8B%E5%AE%B6%7C0%7Cwww.baidu.com%7C2017-05-02+10%3A01%3A31.278%7C2017-05-02+09%3A55%3A56.690; sessionvid=9EC1830B-732B-4FB4-A5E1-17EA664B90EB; area=110199; ahpvno=10")
                .addHeader("Upgrade-Insecure-Requests", "1").end().htmlFetcher()
                .configSingleValueColumn("title", "#articleTitle > h1 > span", null)
                .configSingleValueColumn("content", "#articlecontent", null).end()
                .enbalePageUrlOutput("url")
                .addConstColumnOutput("source", "汽车之家")
                .build();

        Storage storage = new HBaseStorage("xuming.car_article");

        PipeLine pipeLine = new PipeLine(new TimeRangeSheduler(), new TimeRangeRule("0700", "2300"));
        String listUrl = "http://shuoke.autohome.com.cn/%s";
        String listReferUrl = "http://shuoke.autohome.com.cn/%s";
        for (int i = 1; i <= pageCount; i++) {
            String realListUrl = null;
            String realListReferUrl = null;
            if (i == 1) {
                realListUrl = String.format(listUrl, "");
                realListReferUrl = String.format(listReferUrl, "");
            } else {
                realListUrl = String.format(listUrl, i + "/");
                realListReferUrl = String.format(listReferUrl, (i - 1)+"/");
            }

            pipeLine.pipe(realListUrl, realListReferUrl, storage, listPage, detailPage);
        }

        PipeLine.close(storage, countPage, listPage, detailPage);
    }
}
