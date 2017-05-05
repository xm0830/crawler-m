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
import java.util.List;

/**
 * Created by xuming on 2017/5/3.
 */
public class SoHuBBSCrawler {
    public static void main(String[] args) throws IOException {
        PageExtract brandPage = PageExtract.builder().httpSender()
                .addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
                .addHeader("Accept-Encoding", "gzip, deflate, sdch")
                .addHeader("Accept-Language", "zh-CN,zh;q=0.8")
                .addHeader("Cache-Control", "max-age=0")
                .addHeader("Cookie", "beans_12539=visit:2; beans_13981=visit:1; vjuids=7ea2638f.15962e587bf.0.a0a1f0dd7e41c; IPLOC=CN3300; SUV=1612081147280535; JSESSIONID=aaaYJsyka7PexgZG8hiVv; Hm_lvt_6dac3c1faf05a37d54f96d717056834d=1493790631; Hm_lpvt_6dac3c1faf05a37d54f96d717056834d=1493793161; sohutag=8HsmeSc5NCwmcyc5NCwmYjc5NSwmYSc5OSwmZjc5NCwmZyc5NCwmbjc5NCwmaSc5NCwmdyc5NCwmaCc5NCwmYyc5NCwmZSc5NCwmbSc5NCwmdCc5NH0; __utma=32066017.2025789650.1493790610.1493790610.1493794299.2; __utmb=32066017.1.10.1493794299; __utmc=32066017; __utmz=32066017.1493794299.2.2.utmcsr=db.auto.sohu.com|utmccn=(referral)|utmcmd=referral|utmcct=/model_2374/; vjlast=1483422927.1493790294.23; records_models=%u5927%u4F17%u6717%u884C%7C%7C3998%3B%u5965%u8FEAA4L%7C%7C2374; ipcncode=CN110000")
                .addHeader("Upgrade-Insecure-Requests", "1").end().htmlFetcher()
                .configMultiValueColumn("url", "#treeNav > ul.tree > li.close_child > h4 > a", "href")
                .configValueProcessor("url", new Processor() {
                    @Override
                    public String process(String value, String url) {
                        return "http:" + value;
                    }
                }).end()
                .build();

        PageExtract carPage = PageExtract.builder().httpSender()
                .addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
                .addHeader("Accept-Encoding", "gzip, deflate, sdch")
                .addHeader("Accept-Language", "zh-CN,zh;q=0.8")
                .addHeader("Cookie", "beans_12539=visit:1; beans_13981=visit:2; vjuids=7ea2638f.15962e587bf.0.a0a1f0dd7e41c; IPLOC=CN3300; SUV=1612081147280535; JSESSIONID=aaaYJsyka7PexgZG8hiVv; Hm_lvt_6dac3c1faf05a37d54f96d717056834d=1493790631; Hm_lpvt_6dac3c1faf05a37d54f96d717056834d=1493793161; sohutag=8HsmeSc5NCwmcyc5NCwmYjc5NSwmYSc5OSwmZjc5NCwmZyc5NCwmbjc5NCwmaSc5NCwmdyc5NCwmaCc5NCwmYyc5NCwmZSc5NCwmbSc5NCwmdCc5NH0; __utma=32066017.2025789650.1493790610.1493790610.1493794299.2; __utmb=32066017.1.10.1493794299; __utmc=32066017; __utmz=32066017.1493794299.2.2.utmcsr=db.auto.sohu.com|utmccn=(referral)|utmcmd=referral|utmcct=/model_2374/; vjlast=1483422927.1493790294.23; records_models=%u5927%u4F17%u6717%u884C%7C%7C3998%3B%u5965%u8FEAA4L%7C%7C2374; ipcncode=CN110000")
                .addHeader("Upgrade-Insecure-Requests", "1").end().htmlFetcher()
                .configMultiValueColumn("url", "#ucon_1 > div.tabcon > ul > li > a", "href")
                .configValueProcessor("url", new Processor() {
                    @Override
                    public String process(String value, String url) {
                        return "http:" + value;
                    }
                }).end()
                .build();

        PageExtract detailPage = PageExtract.builder().httpSender()
                .addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
                .addHeader("Accept-Encoding", "gzip, deflate, sdch")
                .addHeader("Accept-Language", "zh-CN,zh;q=0.8")
                .addHeader("Cache-Control", "max-age=0")
                .addHeader("Cookie", "beans_11266=visit:1; beans_13641=visit:1; beans_13642=visit:2; beans_11266=visit:1; beans_13641=visit:1; vjuids=7ea2638f.15962e587bf.0.a0a1f0dd7e41c; IPLOC=CN3300; SUV=1612081147280535; JSESSIONID=aaaYJsyka7PexgZG8hiVv; Hm_lvt_6dac3c1faf05a37d54f96d717056834d=1493790631; Hm_lpvt_6dac3c1faf05a37d54f96d717056834d=1493793161; sohutag=8HsmeSc5NCwmcyc5NCwmYjc5NSwmYSc5OSwmZjc5NCwmZyc5NCwmbjc5NCwmaSc5NCwmdyc5NCwmaCc5NCwmYyc5NCwmZSc5NCwmbSc5NCwmdCc5NH0; __utma=32066017.2025789650.1493790610.1493790610.1493794299.2; __utmc=32066017; __utmz=32066017.1493794299.2.2.utmcsr=db.auto.sohu.com|utmccn=(referral)|utmcmd=referral|utmcct=/model_2374/; vjlast=1483422927.1493790294.23; records_models=%u5954%u817EB50%7C%7C2427%3B%u5927%u4F17%u6717%u884C%7C%7C3998%3B%u5965%u8FEAA4L%7C%7C2374; ipcncode=CN110000")
                .addHeader("Upgrade-Insecure-Requests", "1").end().htmlFetcher()
                .configSingleValueColumn("url", "#model_forum > a", "href").end()
                .build();

        PageExtract listPage = PageExtract.builder().httpSender()
                .addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
                .addHeader("Accept-Encoding", "gzip, deflate, sdch")
                .addHeader("Accept-Language", "zh-CN,zh;q=0.8")
                .addHeader("Cache-Control", "max-age=0")
                .addHeader("Cookie", "vjuids=7ea2638f.15962e587bf.0.a0a1f0dd7e41c; IPLOC=CN3300; SUV=1612081147280535; JSESSIONID=aaaAKvcUoMy_q7iGM1mVv; bdshare_firstime=1493790309644; records_models=%u963F%u5C14%u6CD5%u7F57%u5BC6%u6B27Giulia%7C%7C4829%3B%u5954%u817EB50%7C%7C2427%3B%u5927%u4F17%u6717%u884C%7C%7C3998%3B%u5965%u8FEAA4L%7C%7C2374; __utma=32066017.2025789650.1493790610.1493794299.1493799547.3; __utmc=32066017; __utmz=32066017.1493794299.2.2.utmcsr=db.auto.sohu.com|utmccn=(referral)|utmcmd=referral|utmcct=/model_2374/; Hm_lvt_6dac3c1faf05a37d54f96d717056834d=1493790631; Hm_lpvt_6dac3c1faf05a37d54f96d717056834d=1493799914; sohutag=8HsmeSc5NCwmcyc5NCwmYjc5NSwmYSc5NTcsJ2YmOiAsJ2cmOiAsJ24mOiAsJ2kmOiAsJ3cmOiAsJ2gmOiAsJ2NmOiAsJ2UmOiAsJ20mOiAsJ3QmOiB9; RecentVisitBar=10050-%E5%A5%94%E8%85%BEB50%E8%BD%A6%E5%8F%8B%E4%BC%9A#11677-%E5%A5%A5%E8%BF%AAA4%2FA4L%E8%BD%A6%E5%8F%8B%E4%BC%9A; Hm_lvt_d4a5ea633b373aa7aeebc77f8e862f24=1493801567,1493801652,1493802130,1493802144; Hm_lpvt_d4a5ea633b373aa7aeebc77f8e862f24=1493803296; vjlast=1483422927.1493790294.23")
                .addHeader("Upgrade-Insecure-Requests", "1").end().htmlFetcher()
                .configMultiValueColumn("url", "body > div.tw_bbs_daquan_main > div.tw_bbs_daquan_right > div.tw_bbs_daquan_rbox > table > tbody > tr > td:nth-child(2) > p > a", "href").end()
                .build();

        PageExtract resultPage = PageExtract.builder().httpSender()
                .addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
                .addHeader("Accept-Encoding", "gzip, deflate, sdch")
                .addHeader("Accept-Language", "zh-CN,zh;q=0.8")
                .addHeader("Cache-Control", "max-age=0")
                .addHeader("Cookie", "vjuids=7ea2638f.15962e587bf.0.a0a1f0dd7e41c; IPLOC=CN3300; SUV=1612081147280535; JSESSIONID=aaaAKvcUoMy_q7iGM1mVv; bdshare_firstime=1493790309644; records_models=%u963F%u5C14%u6CD5%u7F57%u5BC6%u6B27Giulia%7C%7C4829%3B%u5954%u817EB50%7C%7C2427%3B%u5927%u4F17%u6717%u884C%7C%7C3998%3B%u5965%u8FEAA4L%7C%7C2374; __utma=32066017.2025789650.1493790610.1493794299.1493799547.3; __utmc=32066017; __utmz=32066017.1493794299.2.2.utmcsr=db.auto.sohu.com|utmccn=(referral)|utmcmd=referral|utmcct=/model_2374/; Hm_lvt_6dac3c1faf05a37d54f96d717056834d=1493790631; Hm_lpvt_6dac3c1faf05a37d54f96d717056834d=1493799914; sohutag=8HsmeSc5NCwmcyc5NCwmYjc5NSwmYSc5NTcsJ2YmOiAsJ2cmOiAsJ24mOiAsJ2kmOiAsJ3cmOiAsJ2gmOiAsJ2NmOiAsJ2UmOiAsJ20mOiAsJ3QmOiB9; RecentVisitBar=10050-%E5%A5%94%E8%85%BEB50%E8%BD%A6%E5%8F%8B%E4%BC%9A#11677-%E5%A5%A5%E8%BF%AAA4%2FA4L%E8%BD%A6%E5%8F%8B%E4%BC%9A; Hm_lvt_d4a5ea633b373aa7aeebc77f8e862f24=1493801567,1493801652,1493802130,1493802144; Hm_lpvt_d4a5ea633b373aa7aeebc77f8e862f24=1493803802; vjlast=1483422927.1493790294.23")
                .addHeader("Upgrade-Insecure-Requests", "1").end().htmlFetcher()
                .configSingleValueColumn("title", "body > div.wapper980 > div.conmain > div.con-head > h1", null)
                .configSingleValueColumn("content", "#floor-0 > div.con-main-wapper > div.con-main > div.main-bd", null)
                .configSingleValueColumn("bbs", "body > div.wapper980 > div.conmain > div.con-head > h1 > a", null)
                .configValueProcessor("title", new Processor() {
                    @Override
                    public String process(String value, String url) {
                        String[] tokens = value.split(">", -1);
                        if (tokens.length == 2) {
                            return tokens[1].trim();
                        }
                        return tokens[0];
                    }
                }).end()
                .enbalePageUrlOutput("url")
                .addConstColumnOutput("source", "搜狐汽车")
                .build();

        PipeLine pipeLine = new PipeLine(new TimeRangeSheduler(), new TimeRangeRule("0700", "2300"));

        String url = "http://db.auto.sohu.com/home/";
        String refer = "http://auto.sohu.com/";
        FetchResult res = pipeLine.pipe(url, refer, brandPage, carPage, detailPage);
        PipeLine.close(brandPage, carPage, detailPage);

        Storage storage = new HBaseStorage("xuming.car_bbs");

        List<String> urls = res.select("url");
        for (int i = 0; i < urls.size(); i++) {
            String tmpUrl = urls.get(i);

            String tplUrl = tmpUrl + "%s";
            String tplRefer = tmpUrl + "%s";
            for (int j = 1; ; j++) {
                String realUrl = null;
                String realRefer = null;
                if (j == 1) {
                    realUrl = String.format(tplUrl, "");
                    realRefer = String.format(tplRefer, "");
                } else {
                    realUrl = String.format(tplUrl, String.format("/all-1-%s.shtml", j));
                    realRefer = String.format(tplRefer, String.format("/all-1-%s.shtml", j-1));
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
