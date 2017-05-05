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
public class SoHuArticleCrawler {
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

        PageExtract articlePage = PageExtract.builder().httpSender()
                .addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
                .addHeader("Accept-Encoding", "gzip, deflate, sdch")
                .addHeader("Accept-Language", "zh-CN,zh;q=0.8")
                .addHeader("Cache-Control", "max-age=0")
                .addHeader("Cookie", "beans_11266=visit:1; beans_13641=visit:1; beans_13642=visit:2; beans_11266=visit:1; beans_13641=visit:1; vjuids=7ea2638f.15962e587bf.0.a0a1f0dd7e41c; IPLOC=CN3300; SUV=1612081147280535; JSESSIONID=aaaYJsyka7PexgZG8hiVv; Hm_lvt_6dac3c1faf05a37d54f96d717056834d=1493790631; Hm_lpvt_6dac3c1faf05a37d54f96d717056834d=1493793161; sohutag=8HsmeSc5NCwmcyc5NCwmYjc5NSwmYSc5OSwmZjc5NCwmZyc5NCwmbjc5NCwmaSc5NCwmdyc5NCwmaCc5NCwmYyc5NCwmZSc5NCwmbSc5NCwmdCc5NH0; __utma=32066017.2025789650.1493790610.1493790610.1493794299.2; __utmc=32066017; __utmz=32066017.1493794299.2.2.utmcsr=db.auto.sohu.com|utmccn=(referral)|utmcmd=referral|utmcct=/model_2374/; vjlast=1483422927.1493790294.23; records_models=%u5954%u817EB50%7C%7C2427%3B%u5927%u4F17%u6717%u884C%7C%7C3998%3B%u5965%u8FEAA4L%7C%7C2374; ipcncode=CN110000")
                .addHeader("Upgrade-Insecure-Requests", "1").end().htmlFetcher()
                .configSingleValueColumn("url", "#model_news > a", "href")
                .configValueProcessor("url", new Processor() {
                    @Override
                    public String process(String value, String url) {
                        return "http:" + value;
                    }
                }).end()
                .build();

        PageExtract itemPage = PageExtract.builder().httpSender()
                .addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
                .addHeader("Accept-Encoding", "gzip, deflate, sdch")
                .addHeader("Accept-Language", "zh-CN,zh;q=0.8")
                .addHeader("Cache-Control", "max-age=0")
                .addHeader("Cookie", "beans_11266=visit:1; beans_13641=visit:1; beans_13642=visit:2; beans_11266=visit:1; beans_13641=visit:1; beans_11266=visit:2; beans_13641=visit:2; beans_13642=visit:1; vjuids=7ea2638f.15962e587bf.0.a0a1f0dd7e41c; IPLOC=CN3300; SUV=1612081147280535; JSESSIONID=aaaYJsyka7PexgZG8hiVv; Hm_lvt_6dac3c1faf05a37d54f96d717056834d=1493790631; Hm_lpvt_6dac3c1faf05a37d54f96d717056834d=1493793161; sohutag=8HsmeSc5NCwmcyc5NCwmYjc5NSwmYSc5OSwmZjc5NCwmZyc5NCwmbjc5NCwmaSc5NCwmdyc5NCwmaCc5NCwmYyc5NCwmZSc5NCwmbSc5NCwmdCc5NH0; __utma=32066017.2025789650.1493790610.1493790610.1493794299.2; __utmc=32066017; __utmz=32066017.1493794299.2.2.utmcsr=db.auto.sohu.com|utmccn=(referral)|utmcmd=referral|utmcct=/model_2374/; vjlast=1483422927.1493790294.23; records_models=%u5954%u817EB50%7C%7C2427%3B%u5927%u4F17%u6717%u884C%7C%7C3998%3B%u5965%u8FEAA4L%7C%7C2374; ipcncode=CN110000")
                .addHeader("Upgrade-Insecure-Requests", "1").end().htmlFetcher()
                .configMultiValueColumn("url", "body > div.area > div.left1 > div > dl > dt > span > a", "href")
                .configValueProcessor("url", new Processor() {
                    @Override
                    public String process(String value, String url) {
                        String rs = null;
                        if (value.equals("javascript:void(0)")) {
                            rs = url;
                        } else {
                            rs = "http:" + value;
                        }

                        return rs.substring(0, rs.lastIndexOf("/") + 1);
                    }
                }).end()
                .build();

        PageExtract listPage = PageExtract.builder().httpSender()
                .addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
                .addHeader("Accept-Encoding", "gzip, deflate, sdch")
                .addHeader("Accept-Language", "zh-CN,zh;q=0.8")
                .addHeader("Cache-Control", "max-age=0")
                .addHeader("Cookie", "beans_11266=visit:1; beans_13641=visit:1; beans_13642=visit:2; beans_11266=visit:1; beans_13641=visit:1; beans_11266=visit:2; beans_13641=visit:2; beans_13642=visit:1; vjuids=7ea2638f.15962e587bf.0.a0a1f0dd7e41c; IPLOC=CN3300; SUV=1612081147280535; JSESSIONID=aaaYJsyka7PexgZG8hiVv; Hm_lvt_6dac3c1faf05a37d54f96d717056834d=1493790631; Hm_lpvt_6dac3c1faf05a37d54f96d717056834d=1493793161; sohutag=8HsmeSc5NCwmcyc5NCwmYjc5NSwmYSc5OSwmZjc5NCwmZyc5NCwmbjc5NCwmaSc5NCwmdyc5NCwmaCc5NCwmYyc5NCwmZSc5NCwmbSc5NCwmdCc5NH0; __utma=32066017.2025789650.1493790610.1493790610.1493794299.2; __utmc=32066017; __utmz=32066017.1493794299.2.2.utmcsr=db.auto.sohu.com|utmccn=(referral)|utmcmd=referral|utmcct=/model_2374/; vjlast=1483422927.1493790294.23; records_models=%u5954%u817EB50%7C%7C2427%3B%u5927%u4F17%u6717%u884C%7C%7C3998%3B%u5965%u8FEAA4L%7C%7C2374; ipcncode=CN110000")
                .addHeader("Upgrade-Insecure-Requests", "1").end().htmlFetcher()
                .configMultiValueColumn("url", "#txt_list > li > a", "href").end()
                .build();

        PageExtract resultPage = PageExtract.builder().httpSender()
                .setCharset("gbk")
                .addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
                .addHeader("Accept-Encoding", "gzip, deflate, sdch")
                .addHeader("Accept-Language", "zh-CN,zh;q=0.8")
                .addHeader("Cache-Control", "max-age=0")
                .addHeader("Cookie", "AutoTongLanCookie=visit:1; TurnAD3721=visit:3; TurnAD3722=visit:2; TurnAD3723=visit:1; TurnAD3724=visit:2; vjuids=7ea2638f.15962e587bf.0.a0a1f0dd7e41c; ab_test_index=old; IPLOC=CN3300; SUV=1612081147280535; TurnAD273=visit:2; TurnAD274=visit:2; TurnAD275=visit:2; __utma=32066017.2025789650.1493790610.1493790610.1493794299.2; __utmc=32066017; __utmz=32066017.1493794299.2.2.utmcsr=db.auto.sohu.com|utmccn=(referral)|utmcmd=referral|utmcct=/model_2374/; TurnAD34=visit:1; TurnAD215=visit:3; TurnADTxt140106=visit:1; TurnAD194=visit:3; TurnAD26070=visit:3; TurnAD228=visit:2; TurnAD2152=visit:2; TurnADqc3tong=visit:3; TurnAD2153=visit:3; TurnAD437=visit:1; TurnAD2154=visit:1; TurnADqc5tong=visit:2; TurnAD2155=visit:2; TurnADqc6tong=visit:2; TurnAD2156=visit:2; TurnADqc7tong=visit:2; TurnAD2157=visit:1; TurnAD301=visit:1; TurnAD302=visit:2; TurnAD303=visit:1; TurnAD304=visit:2; TurnADqc8tong=visit:2; TurnAD2158=visit:2; hotcount=2; records_models=%u5954%u817EB50%7C%7C2427%3B%u5927%u4F17%u6717%u884C%7C%7C3998%3B%u5965%u8FEAA4L%7C%7C2374; ipcncode=CN110000; Hm_lvt_6dac3c1faf05a37d54f96d717056834d=1493790631; Hm_lpvt_6dac3c1faf05a37d54f96d717056834d=1493798054; vjlast=1483422927.1493790294.23; sohutag=8HsmeSc5NCwmcyc5NCwmYjc5NSwmYSc5NTAsJ2YmOiAsJ2cmOiAsJ24mOiAsJ2kmOiAsJ3cmOiAsJ2gmOiAsJ2NmOiAsJ2UmOiAsJ20mOiAsJ3QmOiB9")
                .addHeader("If-Modified-Since", "Wed, 03 May 2017 05:40:03 GMT")
                .addHeader("Upgrade-Insecure-Requests", "1").end().htmlFetcher()
                .configSingleValueColumn("title", "#contentA > div.left > h1, #container > div.content-wrapper > div.news-info > div > h1", null)
                .configSingleValueColumn("content", "#contentText", null).end()
                .enbalePageUrlOutput("url")
                .addConstColumnOutput("source", "搜狐汽车")
                .build();


        PipeLine pipeLine = new PipeLine(new TimeRangeSheduler(), new TimeRangeRule("0700", "2300"));

        String url = "http://db.auto.sohu.com/home/";
        String refer = "http://auto.sohu.com/";
        FetchResult res = pipeLine.pipe(url, refer, brandPage, carPage, articlePage, itemPage);
        PipeLine.close(brandPage, carPage, articlePage, itemPage);

        Storage storage = new HBaseStorage("xuming.car_article");

        List<String> urls = res.select("url");
        for (int i = 0; i < urls.size(); i++) {
            String tpmUrl = urls.get(i);

            String tplUrl = tpmUrl + "%s";
            String tplRefer = tpmUrl + "%s";
            for (int j = 1; ; j++) {
                String realUrl = String.format(tplUrl, "page_" + j + ".html");
                String realRefer = null;
                if (j == 1) {
                    realRefer = String.format(tplRefer, "page_" + j + ".html");
                } else {
                    realRefer = String.format(tplRefer, "page_" + (j-1) + ".html");
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
