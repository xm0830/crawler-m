package com.rainbow.example;

import com.rainbow.api.PageExtract;
import com.rainbow.api.PipeLine;
import com.rainbow.fetcher.JsonFetcher;
import com.rainbow.scheduler.TimeRangeRule;
import com.rainbow.scheduler.TimeRangeSheduler;
import com.rainbow.storage.HBaseStorage;
import com.rainbow.storage.Storage;

import java.io.IOException;
import java.util.Date;

/**
 * Created by xuming on 2017/5/3.
 */
public class SoHuNewsCrawler {
    public static void main(String[] args) throws IOException {
        PageExtract listPage = PageExtract.builder().httpSender()
                .addHeader("Accept", "*/*")
                .addHeader("Accept-Encoding", "gzip, deflate, sdch")
                .addHeader("Accept-Language", "zh-CN,zh;q=0.8")
                .addHeader("Cookie", "vjuids=7ea2638f.15962e587bf.0.a0a1f0dd7e41c; IPLOC=CN3300; SUV=1612081147280535; sohu_user_ip=36.110.73.210; records_models=%u5965%u8FEAA4L%7C%7C2374; Hm_lvt_6dac3c1faf05a37d54f96d717056834d=1493790631; Hm_lpvt_6dac3c1faf05a37d54f96d717056834d=1493791054; sohutag=8HsmeSc5NCwmcyc5NCwmYjc5NSwmYSc5MCwmZjc5NCwmZyc5NCwmbjc5NCwmaSc5NCwmdyc5NCwmaCc5NCwmYyc5NCwmZSc5NCwmbSc5NCwmdCc5NH0; vjlast=1483422927.1493790294.23; __utma=32066017.2025789650.1493790610.1493790610.1493790610.1; __utmb=32066017.3.10.1493790610; __utmc=32066017; __utmz=32066017.1493790610.1.1.utmcsr=db.auto.sohu.com|utmccn=(referral)|utmcmd=referral|utmcct=/chezhudianping/; ipcncode=CN110000").end().jsonFetcher()
                .configRootType(JsonFetcher.JSONType.OBJECT)
                .configSelector("url", "result", JsonFetcher.JSONType.ARRAY)
                .configSelector("url", "url", JsonFetcher.JSONType.OBJECT).end()
                .build();

        PageExtract resultPage = PageExtract.builder().httpSender()
                .setCharset("gbk")
                .addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
                .addHeader("Accept-Encoding", "gzip, deflate, sdch")
                .addHeader("Accept-Language", "zh-CN,zh;q=0.8")
                .addHeader("Cache-Control", "max-age=0")
                .addHeader("Cookie", "AutoTongLanCookie=visit:1; TurnAD3721=visit:3; TurnAD3722=visit:3; TurnAD3723=visit:2; TurnAD3724=visit:2; vjuids=7ea2638f.15962e587bf.0.a0a1f0dd7e41c; ab_test_index=old; TurnAD34=visit:1; TurnAD215=visit:3; TurnADTxt140106=visit:1; TurnAD194=visit:3; TurnAD26070=visit:3; TurnAD228=visit:2; TurnAD2152=visit:2; TurnADqc3tong=visit:3; TurnAD2153=visit:3; TurnAD437=visit:1; TurnAD2154=visit:1; TurnADqc5tong=visit:2; TurnAD2155=visit:2; TurnADqc6tong=visit:2; TurnAD2156=visit:2; TurnADqc7tong=visit:2; TurnAD2157=visit:1; TurnAD301=visit:1; TurnAD302=visit:2; TurnAD303=visit:1; TurnAD304=visit:2; TurnADqc8tong=visit:2; TurnAD2158=visit:2; IPLOC=CN3300; SUV=1612081147280535; hotcount=2; sohu_user_ip=36.110.73.210; records_models=%u5965%u8FEAA4L%7C%7C2374; TurnAD273=visit:1; TurnAD274=visit:1; TurnAD275=visit:1; __utma=32066017.2025789650.1493790610.1493790610.1493790610.1; __utmb=32066017.3.10.1493790610; __utmc=32066017; __utmz=32066017.1493790610.1.1.utmcsr=db.auto.sohu.com|utmccn=(referral)|utmcmd=referral|utmcct=/chezhudianping/; ipcncode=CN110000; Hm_lvt_6dac3c1faf05a37d54f96d717056834d=1493790631; Hm_lpvt_6dac3c1faf05a37d54f96d717056834d=1493791843; vjlast=1483422927.1493790294.23; sohutag=8HsmeSc5NCwmcyc5NCwmYjc5NSwmYSc5MSwmZjc5NCwmZyc5NCwmbjc5NCwmaSc5NCwmdyc5NCwmaCc5NCwmYyc5NCwmZSc5NCwmbSc5NCwmdCc5NH0")
                .addHeader("If-Modified-Since", "Sun, 30 Apr 2017 01:01:24 GMT")
                .addHeader("Upgrade-Insecure-Requests", "1").end().htmlFetcher()
                .configSingleValueColumn("title", "#contentA > div.left > h1", null)
                .configSingleValueColumn("content", "#contentText", null).end()
                .enbalePageUrlOutput("url")
                .addConstColumnOutput("source", "搜狐汽车")
                .build();

        PipeLine pipeLine = new PipeLine(new TimeRangeSheduler(), new TimeRangeRule("0700", "2300"));

        Storage storage = new HBaseStorage("xuming.car_article");

        String url = "http://api.db.auto.sohu.com/restful/news/list/news/%s/20.json?callback=news&_=%s";
        String refer = "http://auto.sohu.com/qichexinwen.shtml";

        for (int i=1; ; i++){
            String realUrl = String.format(url, i, new Date().getTime());
            long count = pipeLine.pipe(realUrl, refer, storage, listPage, resultPage);
            if (count <= 0) {
                break;
            }
        }

        PipeLine.close(storage, listPage, resultPage);
    }
}
