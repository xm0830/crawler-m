package com.rainbow.example;

import com.rainbow.api.PageExtract;
import com.rainbow.api.PipeLine;
import com.rainbow.fetcher.Processor;
import com.rainbow.scheduler.TimeRangeRule;
import com.rainbow.scheduler.TimeRangeSheduler;
import com.rainbow.storage.HBaseStorage;
import com.rainbow.storage.Storage;

import java.io.IOException;

/**
 * Created by xuming on 2017/5/5.
 */
public class QCDQArticleCrawler {
    public static void main(String[] args) throws IOException {
        PageExtract brandPage = PageExtract.builder().httpSender()
                .addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
                .addHeader("Accept-Encoding", "gzip, deflate, sdch")
                .addHeader("Accept-Language", "zh-CN,zh;q=0.8")
                .addHeader("Cache-Control", "max-age=0")
                .addHeader("Cookie", "xyautoarea=201_%E5%8C%97%E4%BA%AC; xy_id=4e4c568840eb44afa6b027435e330bec; doubleADCookie=%2C5649%3A2%2C5660%3A0%2C5666%3A0%2C5669%3A0%2C5670%3A0%2C5668%3A0; XYAUTOWEBLOG_testcookie=yes; _cu_207947254=0.0.0.1%7C9380504374z60p6812%7C1493833843747%7C8%7C0%7C0%7C4; _cs_207947254=9397089368gv5myj16%7C8%7C1493999693681; _cbrs_207947254=1; Hm_lvt_b8e99a897408d379d2b93c5359ffd8c0=1493805044,1493864398,1493963138,1493970894; Hm_lpvt_b8e99a897408d379d2b93c5359ffd8c0=1493971030")
                .addHeader("Upgrade-Insecure-Requests", "1").end().htmlFetcher()
                .configMultiValueColumn("url", "#treev1 > ul > li > ul > li > a", "href")
                .configValueProcessor("url", new Processor() {
                    @Override
                    public String process(String value, String url) {
                        return "http://car.qichedaquan.com" + value.trim();
                    }
                }).end()
                .build();

        PageExtract carPage = PageExtract.builder().httpSender()
                .addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
                .addHeader("Accept-Encoding", "gzip, deflate, sdch")
                .addHeader("Accept-Language", "zh-CN,zh;q=0.8")
                .addHeader("Cache-Control", "max-age=0")
                .addHeader("Cookie", "xyautoarea=201_%E5%8C%97%E4%BA%AC; xy_id=4e4c568840eb44afa6b027435e330bec; doubleADCookie=%2C5649%3A2%2C5660%3A0%2C5666%3A0%2C5669%3A0%2C5670%3A0%2C5668%3A0; XYAUTOWEBLOG_testcookie=yes; _cu_207947254=0.0.0.1%7C9380504374z60p6812%7C1493833843747%7C8%7C0%7C0%7C4; _cs_207947254=9397089368gv5myj16%7C8%7C1493999693681; _cbrs_207947254=1; Hm_lvt_b8e99a897408d379d2b93c5359ffd8c0=1493805044,1493864398,1493963138,1493970894; Hm_lpvt_b8e99a897408d379d2b93c5359ffd8c0=1493971030")
                .addHeader("Upgrade-Insecure-Requests", "1").end().htmlFetcher()
                .configMultiValueColumn("url", "div.treeMainv1 > div.data_level > ul.brand_car > li > a:nth-child(2)", "href")
                .configValueProcessor("url", new Processor() {
                    @Override
                    public String process(String value, String url) {
                        return "http://car.qichedaquan.com" + value.trim();
                    }
                }).end()
                .build();

        PageExtract menuPage = PageExtract.builder().httpSender()
                .addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
                .addHeader("Accept-Encoding", "gzip, deflate, sdch")
                .addHeader("Accept-Language", "zh-CN,zh;q=0.8")
                .addHeader("Cache-Control", "max-age=0")
                .addHeader("Cookie", "XYAUTOWEBLOG_testcookie=yes; xyautoarea=201_%E5%8C%97%E4%BA%AC; xy_id=4e4c568840eb44afa6b027435e330bec; doubleADCookie=%2C5649%3A2%2C5660%3A0%2C5666%3A0%2C5669%3A0%2C5670%3A0%2C5668%3A0; XYAUTOWEBLOG_testcookie=yes; Hm_lvt_b8e99a897408d379d2b93c5359ffd8c0=1493805044,1493864398,1493963138,1493970894; Hm_lpvt_b8e99a897408d379d2b93c5359ffd8c0=1493972704; _cu_207947254=0.0.0.1%7C9380504374z60p6812%7C1493833843747%7C8%7C0%7C0%7C4; _cs_207947254=9397089368gv5myj16%7C11%7C1493999693681; _cbrs_207947254=1")
                .addHeader("Upgrade-Insecure-Requests", "1").end().htmlFetcher()
                .configMultiValueColumn("url", "div.container > div > div.summary_imgs_c_title > div > ul > li:nth-child(7) > a", "href").end()
                .build();

        PageExtract listPage = PageExtract.builder().httpSender()
                .addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
                .addHeader("Accept-Encoding", "gzip, deflate, sdch")
                .addHeader("Accept-Language", "zh-CN,zh;q=0.8")
                .addHeader("Cache-Control", "max-age=0")
                .addHeader("Cookie", "XYAUTOWEBLOG_testcookie=yes; xyautoarea=201_%E5%8C%97%E4%BA%AC; xy_id=4e4c568840eb44afa6b027435e330bec; doubleADCookie=%2C5649%3A2%2C5660%3A0%2C5666%3A0%2C5669%3A0%2C5670%3A0%2C5668%3A0; XYAUTOWEBLOG_testcookie=yes; Hm_lvt_b8e99a897408d379d2b93c5359ffd8c0=1493805044,1493864398,1493963138,1493970894; Hm_lpvt_b8e99a897408d379d2b93c5359ffd8c0=1493973346; _cu_207947254=0.0.0.1%7C9380504374z60p6812%7C1493833843747%7C8%7C0%7C0%7C4; _cs_207947254=9397089368gv5myj16%7C44%7C1493999693681; _cbrs_207947254=1")
                .addHeader("Upgrade-Insecure-Requests", "1").end().htmlFetcher()
                .configMultiValueColumn("url", "#car_serial_news_content > li > a", "href").end()
                .build();

        PageExtract resultPage = PageExtract.builder().httpSender()
                .addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
                .addHeader("Accept-Encoding", "gzip, deflate, sdch")
                .addHeader("Accept-Language", "zh-CN,zh;q=0.8")
                .addHeader("Cache-Control", "max-age=0")
                .addHeader("Cookie", "XYAUTOWEBLOG_testcookie=yes; xyautoarea=201_%E5%8C%97%E4%BA%AC; xy_id=4e4c568840eb44afa6b027435e330bec; doubleADCookie=%2C5649%3A2%2C5660%3A0%2C5666%3A0%2C5669%3A0%2C5670%3A0%2C5668%3A0; Hm_lvt_b8e99a897408d379d2b93c5359ffd8c0=1493805044,1493864398,1493963138,1493970894; Hm_lpvt_b8e99a897408d379d2b93c5359ffd8c0=1493973613; _cu_207947254=0.0.0.1%7C9380504374z60p6812%7C1493833843747%7C8%7C0%7C0%7C4; _cs_207947254=9397089368gv5myj16%7C51%7C1493999693681; _cbrs_207947254=1")
                .addHeader("Upgrade-Insecure-Requests", "1").end().htmlFetcher()
                .configSingleValueColumn("title", "#p_1", null)
                .configSingleValueColumn("content", "#div > div.box > div.car-type-introduce-con > div.iframe_content", null).end()
                .enbalePageUrlOutput("url")
                .addConstColumnOutput("source", "汽车大全")
                .build();

        PipeLine pipeLine = new PipeLine(new TimeRangeSheduler(), new TimeRangeRule("0700", "2300"));

        Storage storage = new HBaseStorage("xuming.car_article");

        String rootUrl = "http://car.qichedaquan.com/";
        String rootRefer = "http://www.qichedaquan.com/?utm_source=baidu";
        pipeLine.pipe(rootUrl, rootRefer, storage, brandPage, carPage, menuPage, listPage, resultPage);
        PipeLine.close(storage, brandPage, carPage, menuPage, listPage, resultPage);
    }

}
