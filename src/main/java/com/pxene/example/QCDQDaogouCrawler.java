package com.pxene.example;

import com.pxene.api.PageExtract;
import com.pxene.api.PipeLine;
import com.pxene.fetcher.JsonFetcher;
import com.pxene.scheduler.TimeRangeRule;
import com.pxene.scheduler.TimeRangeSheduler;
import com.pxene.storage.HBaseStorage;
import com.pxene.storage.Storage;

import java.io.IOException;

/**
 * Created by xuming on 2017/5/5.
 */
public class QCDQDaogouCrawler {
    public static void main(String[] args) throws IOException {
        PageExtract listPage = PageExtract.builder().httpSender()
                .addHeader("Accept", "*/*")
                .addHeader("Accept-Encoding", "gzip, deflate, sdch")
                .addHeader("Accept-Language", "zh-CN,zh;q=0.8")
                .addHeader("Cookie", "XYAUTOWEBLOG_testcookie=yes; xyautoarea=201_%E5%8C%97%E4%BA%AC; xy_id=4e4c568840eb44afa6b027435e330bec; doubleADCookie=%2C5649%3A2%2C5660%3A0%2C5666%3A0%2C5669%3A0%2C5670%3A0%2C5668%3A0; Hm_lvt_b8e99a897408d379d2b93c5359ffd8c0=1493805044,1493864398,1493963138,1493970894; Hm_lpvt_b8e99a897408d379d2b93c5359ffd8c0=1493975031; _cu_207947254=0.0.0.1%7C9380504374z60p6812%7C1493833843747%7C8%7C0%7C0%7C4; _cs_207947254=9397089368gv5myj16%7C58%7C1493999693681; _cbrs_207947254=1")
                .addHeader("X-Requested-With", "XMLHttpRequest")
                .addHeader("X-Tingyun-Id", "-iGcvmwiT2w;r=75040937")
                .enableAjax().end().jsonFetcher()
                .configRootType(JsonFetcher.JSONType.OBJECT)
                .configSelector("url", "data", JsonFetcher.JSONType.OBJECT)
                .configSelector("url", "newsList", JsonFetcher.JSONType.ARRAY)
                .configSelector("url", "url", JsonFetcher.JSONType.OBJECT).end()
                .build();

        PageExtract resultPage = PageExtract.builder().httpSender()
                .addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
                .addHeader("Accept-Encoding", "gzip, deflate, sdch")
                .addHeader("Accept-Language", "zh-CN,zh;q=0.8")
                .addHeader("Cache-Control", "max-age=0")
                .addHeader("Cookie", "XYAUTOWEBLOG_testcookie=yes; xyautoarea=201_%E5%8C%97%E4%BA%AC; xy_id=4e4c568840eb44afa6b027435e330bec; doubleADCookie=%2C5649%3A2%2C5660%3A0%2C5666%3A0%2C5669%3A0%2C5670%3A0%2C5668%3A0; Hm_lvt_b8e99a897408d379d2b93c5359ffd8c0=1493805044,1493864398,1493963138,1493970894; Hm_lpvt_b8e99a897408d379d2b93c5359ffd8c0=1493975746; _cu_207947254=0.0.0.1%7C9380504374z60p6812%7C1493833843747%7C8%7C0%7C0%7C4; _cs_207947254=9397089368gv5myj16%7C59%7C1493999693681; _cbrs_207947254=1")
                .addHeader("Upgrade-Insecure-Requests", "1").end().htmlFetcher()
                .configSingleValueColumn("title", "#p_1, #div > div.box > div.car-type-introduce-con > h2", null)
                .configSingleValueColumn("content", "#div > div.box > div.car-type-introduce-con > div.iframe_content", null).end()
                .enbalePageUrlOutput("url")
                .addConstColumnOutput("source", "汽车大全")
                .build();

        PipeLine pipeLine = new PipeLine(new TimeRangeSheduler(), new TimeRangeRule("0700", "2300"));

        Storage storage = new HBaseStorage("xuming.car_article");

        String tplUrl = "http://news.qichedaquan.com/daogou/nextpage?pageNo=%s&ext=%s";
        String refer = "http://news.qichedaquan.com/daogou/";

        for (int i = 0; i < 4; i++) {
            for (int j = 1; ; j++) {
                String url = String.format(tplUrl, j, i);
                long count = pipeLine.pipe(url, refer, storage, listPage, resultPage);

                if (count <= 0) {
                    break;
                }
            }
        }

        PipeLine.close(storage, listPage, resultPage);
    }
}
