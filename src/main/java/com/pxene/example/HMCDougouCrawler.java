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
 * Created by xuming on 2017/5/5.
 */
public class HMCDougouCrawler {
    public static void main(String[] args) throws IOException {
        PageExtract menuPage = PageExtract.builder().httpSender()
                .addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
                .addHeader("Accept-Encoding", "gzip, deflate, sdch")
                .addHeader("Accept-Language", "zh-CN,zh;q=0.8")
                .addHeader("Cookie", "XCWEBLOG_testcookie=yes; authkey=d7f776d40b054649971435f4ddabbb10; cusloced=1; hmc_favcs=1994,3152,1909; hmc_favcar=119392,123298,120230; tracker_u=42_bdsempc; hmc_mt=1493977061860; XCWEBLOG_testcookie=yes; Hm_lvt_34b070f600a54baeebad2039a605f793=1493170116,1493262844,1493281753,1493977062; Hm_lpvt_34b070f600a54baeebad2039a605f793=1493977735; Hm_lvt_d877be9ebbae0bd16202a42d41e09f3e=1493170115,1493262843,1493281753,1493977062; Hm_lpvt_d877be9ebbae0bd16202a42d41e09f3e=1493977735; loc=901%7C%E7%9F%B3%E5%AE%B6%E5%BA%84")
                .addHeader("Upgrade-Insecure-Requests", "1").end().htmlFetcher()
                .configMultiValueColumn("url", "#news_tabcon > li > a", "href")
                .configValueProcessor("url", new Processor() {
                    @Override
                    public String process(String value, String url) {
                        return "http://news.huimaiche.com" + value.substring(0, value.lastIndexOf("#"));
                    }
                })
                .end().build();

        PageExtract listPage = PageExtract.builder().httpSender()
                .addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
                .addHeader("Accept-Encoding", "gzip, deflate, sdch")
                .addHeader("Accept-Language", "zh-CN,zh;q=0.8")
                .addHeader("Cookie", "XCWEBLOG_testcookie=yes; authkey=d7f776d40b054649971435f4ddabbb10; cusloced=1; hmc_favcs=1994,3152,1909; hmc_favcar=119392,123298,120230; tracker_u=42_bdsempc; hmc_mt=1493977061860; XCWEBLOG_testcookie=yes; Hm_lvt_34b070f600a54baeebad2039a605f793=1493170116,1493262844,1493281753,1493977062; Hm_lpvt_34b070f600a54baeebad2039a605f793=1493977735; Hm_lvt_d877be9ebbae0bd16202a42d41e09f3e=1493170115,1493262843,1493281753,1493977062; Hm_lpvt_d877be9ebbae0bd16202a42d41e09f3e=1493977735; loc=901%7C%E7%9F%B3%E5%AE%B6%E5%BA%84")
                .addHeader("Upgrade-Insecure-Requests", "1").end().htmlFetcher()
                .configMultiValueColumn("url", "body > div.news_wp > div.news_left > div.news_con_list > ul > li > a.block", "href")
                .configValueProcessor("url", new Processor() {
                    @Override
                    public String process(String value, String url) {
                        return "http://news.huimaiche.com" + value;
                    }
                })
                .end().build();

        PageExtract resultPage = PageExtract.builder().httpSender()
                .addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
                .addHeader("Accept-Encoding", "gzip, deflate, sdch")
                .addHeader("Accept-Language", "zh-CN,zh;q=0.8")
                .addHeader("Cookie", "XCWEBLOG_testcookie=yes; authkey=d7f776d40b054649971435f4ddabbb10; cusloced=1; hmc_favcs=1994,3152,1909; hmc_favcar=119392,123298,120230; tracker_u=42_bdsempc; hmc_mt=1493977061860; XCWEBLOG_testcookie=yes; Hm_lvt_34b070f600a54baeebad2039a605f793=1493170116,1493262844,1493281753,1493977062; Hm_lpvt_34b070f600a54baeebad2039a605f793=1493977402; Hm_lvt_d877be9ebbae0bd16202a42d41e09f3e=1493170115,1493262843,1493281753,1493977062; Hm_lpvt_d877be9ebbae0bd16202a42d41e09f3e=1493977403; loc=901%7C%E7%9F%B3%E5%AE%B6%E5%BA%84")
                .addHeader("Upgrade-Insecure-Requests", "1").end().htmlFetcher()
                .configSingleValueColumn("title", "div.news_wp > div.news_main > h1", null)
                .configSingleValueColumn("content", "div.news_wp > div.news_main > div.news_article", null).end()
                .enbalePageUrlOutput("url")
                .addConstColumnOutput("source", "惠买车")
                .build();

        PipeLine pipeLine = new PipeLine(new TimeRangeSheduler(), new TimeRangeRule("0700", "2300"));

        Storage storage = new HBaseStorage("xuming.car_article");

        String rootUrl = "http://news.huimaiche.com/";
        String rootRefer = "http://shijiazhuang.huimaiche.com/?tracker_u=42_bdsempc";

        FetchResult result = pipeLine.pipe(rootUrl, rootRefer, menuPage);
        PipeLine.close(menuPage);

        List<String> urls = result.select("url");
        for (String url : urls) {
            String tmpUrl = null;
            String tmpRefer = null;

            for (int i = 1; ; i++) {
                if (i == 1) {
                    tmpUrl = url;
                    tmpRefer = url;
                } else {
                    tmpUrl = url + i + "/";
                    tmpRefer = url + (i-1) + "/";
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
