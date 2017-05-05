package com.pxene.example;

import com.pxene.api.PageExtract;
import com.pxene.api.PipeLine;
import com.pxene.fetcher.FetchResult;
import com.pxene.fetcher.Processor;
import com.pxene.scheduler.TimeRangeRule;
import com.pxene.scheduler.TimeRangeSheduler;
import com.pxene.storage.HBaseStorage;
import com.pxene.storage.Storage;
import com.pxene.storage.Storage4Test;

import java.io.IOException;
import java.util.List;

/**
 * Created by xuming on 2017/5/4.
 */
public class SinaTryCarCrawler {
    public static void main(String[] args) throws IOException {
        PageExtract menuPage = PageExtract.builder().httpSender()
                .addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
                .addHeader("Accept-Encoding", "gzip, deflate, sdch")
                .addHeader("Accept-Language", "zh-CN,zh;q=0.8")
                .addHeader("Cache-Control", "max-age=0")
                .addHeader("Cookie", "U_TRS1=000000d2.df6d6ecf.5848d7c8.2200c241; UOR=www.baidu.com,blog.sina.com.cn,; SINAGLOBAL=36.110.73.210_1481168840.624321; vjuids=-7bcf0aa69.158dc8a72ad.0.3719f836d1229; SUB=_2AkMvMwfIf8NhqwJRmP4WxGnmZYR-yAjEieKZb_YTJRMyHRl-yD9kqmM7tRBRvvQedpS5AUauCJP2x14X4eBuJA..; SUBP=0033WrSXqPxfM72-Ws9jqgMF55529P9D9WhAurvfJyZFYJiLgXj1ShrD; U_TRS2=000000d2.1e13760.59093c92.420e5d29; SessionID=u1pq898uio3qt3p63hkig6l3v5; Apache=36.110.73.210_1493864745.789343; ULV=1493864839090:4:1:1:36.110.73.210_1493864745.789343:1483704614105; bdshare_firstime=1493864841014; Hm_lvt_e838058b5eb014b57f3f063e8db1f76d=1493865806; Hm_lpvt_e838058b5eb014b57f3f063e8db1f76d=1493865806; vjlast=1493877844.1493877945.10; Hm_lvt_bf62b1c294436ada9b1c3f3afc41dc9f=1493864799; Hm_lpvt_bf62b1c294436ada9b1c3f3afc41dc9f=1493877998; lxlrttp=1493192605")
                .addHeader("Upgrade-Insecure-Requests", "1").end().htmlFetcher()
                .configMultiValueColumn("url", "#evaluationLink > li > a", "href").end()
                .build();


        PageExtract listPage = PageExtract.builder().httpSender()
                .addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
                .addHeader("Accept-Encoding", "gzip, deflate, sdch")
                .addHeader("Accept-Language", "zh-CN,zh;q=0.8")
                .addHeader("Cache-Control", "max-age=0")
                .addHeader("Cookie", "U_TRS1=000000d2.df6d6ecf.5848d7c8.2200c241; UOR=www.baidu.com,blog.sina.com.cn,; SINAGLOBAL=36.110.73.210_1481168840.624321; vjuids=-7bcf0aa69.158dc8a72ad.0.3719f836d1229; SUB=_2AkMvMwfIf8NhqwJRmP4WxGnmZYR-yAjEieKZb_YTJRMyHRl-yD9kqmM7tRBRvvQedpS5AUauCJP2x14X4eBuJA..; SUBP=0033WrSXqPxfM72-Ws9jqgMF55529P9D9WhAurvfJyZFYJiLgXj1ShrD; U_TRS2=000000d2.1e13760.59093c92.420e5d29; SessionID=u1pq898uio3qt3p63hkig6l3v5; Apache=36.110.73.210_1493864745.789343; ULV=1493864839090:4:1:1:36.110.73.210_1493864745.789343:1483704614105; bdshare_firstime=1493864841014; Hm_lvt_e838058b5eb014b57f3f063e8db1f76d=1493865806; Hm_lpvt_e838058b5eb014b57f3f063e8db1f76d=1493865806; vjlast=1493877844.1493877945.10; Hm_lvt_bf62b1c294436ada9b1c3f3afc41dc9f=1493864799; Hm_lpvt_bf62b1c294436ada9b1c3f3afc41dc9f=1493878299; lxlrttp=1493192605")
                .addHeader("Upgrade-Insecure-Requests", "1").end().htmlFetcher()
                .configMultiValueColumn("url", "div.content > div.con > div.single > div.s-left > h3 > a, div.wrap > div.w_l > ul > li > p > a", "href").end()
                .build();

        PageExtract resultPage = PageExtract.builder().httpSender()
                .addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
                .addHeader("Accept-Encoding", "gzip, deflate, sdch")
                .addHeader("Accept-Language", "zh-CN,zh;q=0.8")
                .addHeader("Cache-Control", "max-age=0")
                .addHeader("Cookie", "U_TRS1=000000d2.df6d6ecf.5848d7c8.2200c241; UOR=www.baidu.com,blog.sina.com.cn,; SINAGLOBAL=36.110.73.210_1481168840.624321; vjuids=-7bcf0aa69.158dc8a72ad.0.3719f836d1229; SUB=_2AkMvMwfIf8NhqwJRmP4WxGnmZYR-yAjEieKZb_YTJRMyHRl-yD9kqmM7tRBRvvQedpS5AUauCJP2x14X4eBuJA..; SUBP=0033WrSXqPxfM72-Ws9jqgMF55529P9D9WhAurvfJyZFYJiLgXj1ShrD; U_TRS2=000000d2.1e13760.59093c92.420e5d29; SessionID=u1pq898uio3qt3p63hkig6l3v5; Apache=36.110.73.210_1493864745.789343; ULV=1493864839090:4:1:1:36.110.73.210_1493864745.789343:1483704614105; bdshare_firstime=1493864841014; Hm_lvt_e838058b5eb014b57f3f063e8db1f76d=1493865806; Hm_lpvt_e838058b5eb014b57f3f063e8db1f76d=1493865806; vjlast=1493877844.1493877945.10; Hm_lvt_bf62b1c294436ada9b1c3f3afc41dc9f=1493864799; Hm_lpvt_bf62b1c294436ada9b1c3f3afc41dc9f=1493879218; lxlrttp=1493192605")
                .addHeader("Upgrade-Insecure-Requests", "1").end().htmlFetcher()
                .configSingleValueColumn("title", "#artibodyTitle, #j_articleContent > h1", null)
                .configSingleValueColumn("content", "#articleContent, #j_articleContent > div.p", null)
                .configValueProcessor("content", new Processor() {
                    @Override
                    public String process(String value, String url) {
                        return value.replaceAll("更多图片 秒车价\\|配置\\|图库\\| 视频\\|口碑\\|", "")
                                .replaceAll("\\|\\|\\|\\|\\| 经销商.+上市询价", "")
                                .replaceAll("经销商.+上市询价", "")
                                .replaceAll("经销商.+万元询价", "");
                    }
                }).end()
                .enbalePageUrlOutput("url")
                .addConstColumnOutput("source", "新浪汽车")
                .build();

        PipeLine pipeLine = new PipeLine(new TimeRangeSheduler(), new TimeRangeRule("0700", "2300"));

        String rootUrl = "http://auto.sina.com.cn/review/index.d.html";
        String rootRefer = "http://auto.sina.com.cn/";
        FetchResult result = pipeLine.pipe(rootUrl, rootRefer, menuPage);
        PipeLine.close(menuPage);

        Storage storage = new HBaseStorage("xuming.car_article");

        List<String> urls = result.select("url");
        for (String url : urls) {

            for (int i = 1; ; i++) {
                String tmpUrl = null;
                String tmpRefer = null;

                if (i == 1) {
                    tmpUrl = url;
                    tmpRefer = url;
                } else {
                    if (url.endsWith(".html")) {
                        tmpUrl = url + "?page=" + i;
                        tmpRefer = url + "?page=" + (i-1);
                    } else {
                        tmpUrl = url + i + ".shtml";
                        tmpRefer = url + (i-1) + ".shtml";
                    }
                }

                long count = pipeLine.pipe(tmpUrl, tmpRefer, storage, listPage, resultPage);
                if (count <= 0) {
                    break;
                }
            }
        }

        PipeLine.close(storage, listPage, resultPage);
    }
}