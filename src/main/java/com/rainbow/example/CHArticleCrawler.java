package com.rainbow.example;

import com.rainbow.api.PageExtract;
import com.rainbow.api.PipeLine;
import com.rainbow.fetcher.FetchResult;
import com.rainbow.scheduler.TimeRangeRule;
import com.rainbow.scheduler.TimeRangeSheduler;
import com.rainbow.storage.HBaseStorage;
import com.rainbow.storage.Storage;

import java.io.IOException;
import java.util.List;

/**
 * Created by xuming on 2017/5/2.
 */
public class CHArticleCrawler {
    public static void main(String[] args) throws IOException {
        PageExtract indexPage = PageExtract.builder().httpSender()
                .addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
                .addHeader("Accept-Encoding", "gzip, deflate, sdch")
                .addHeader("Accept-Language", "zh-CN,zh;q=0.8")
                .addHeader("Cookie", "fvlid=1493109044271mQd2SHqO; sessionip=36.110.73.210; sessionid=6C536D37-02C1-463A-B5FF-C76E17C4AFAC%7C%7C2017-04-25+16%3A30%3A50.191%7C%7Cwww.baidu.com; ahpau=1; mallsfvi=1493367299924jBMxRTcd%7Cwww.autohome.com.cn%7C103414; ASP.NET_SessionId=wjwfdovfcncyqzzgooxpqoqw; ahpvno=88; sessionuid=6C536D37-02C1-463A-B5FF-C76E17C4AFAC%7C%7C2017-04-25+16%3A30%3A50.191%7C%7Cwww.baidu.com; __utma=1.1033022094.1493109047.1493690212.1493705579.5; __utmb=1.0.10.1493705579; __utmc=1; __utmz=1.1493690212.4.3.utmcsr=sogou|utmccn=(organic)|utmcmd=organic|utmctr=%E6%B1%BD%E8%BD%A6%E4%B9%8B%E5%AE%B6; ref=www.sogou.com%7C%E6%B1%BD%E8%BD%A6%E4%B9%8B%E5%AE%B6%7C0%7Cwww.baidu.com%7C2017-05-02+14%3A46%3A09.198%7C2017-05-02+09%3A55%3A56.690; sessionvid=A10D1092-3CF1-4666-9EE8-221E34FF51D6; area=110199; ahrlid=1493707620950mklO78ZM-1493707640765")
                .addHeader("Upgrade-Insecure-Requests", "1").end().htmlFetcher()
                .configSingleValueColumn("url", "#auto-header-news-list > dl > dd:nth-child(1) > a", "href").end()
                .build();

        PageExtract countPage = PageExtract.builder().httpSender()
                .addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
                .addHeader("Accept-Encoding", "gzip, deflate, sdch")
                .addHeader("Accept-Language", "zh-CN,zh;q=0.8")
                .addHeader("Cookie", "fvlid=1493109044271mQd2SHqO; sessionip=36.110.73.210; sessionid=6C536D37-02C1-463A-B5FF-C76E17C4AFAC%7C%7C2017-04-25+16%3A30%3A50.191%7C%7Cwww.baidu.com; ahpau=1; mallsfvi=1493367299924jBMxRTcd%7Cwww.autohome.com.cn%7C103414; ASP.NET_SessionId=wjwfdovfcncyqzzgooxpqoqw; ahpvno=88; sessionuid=6C536D37-02C1-463A-B5FF-C76E17C4AFAC%7C%7C2017-04-25+16%3A30%3A50.191%7C%7Cwww.baidu.com; __utma=1.1033022094.1493109047.1493690212.1493705579.5; __utmb=1.0.10.1493705579; __utmc=1; __utmz=1.1493690212.4.3.utmcsr=sogou|utmccn=(organic)|utmcmd=organic|utmctr=%E6%B1%BD%E8%BD%A6%E4%B9%8B%E5%AE%B6; ref=www.sogou.com%7C%E6%B1%BD%E8%BD%A6%E4%B9%8B%E5%AE%B6%7C0%7Cwww.baidu.com%7C2017-05-02+14%3A46%3A09.198%7C2017-05-02+09%3A55%3A56.690; sessionvid=A10D1092-3CF1-4666-9EE8-221E34FF51D6; area=110199; ahrlid=1493707620950mklO78ZM-1493707640765")
                .addHeader("Upgrade-Insecure-Requests", "1").end().htmlFetcher()
                .configSingleValueColumn("count", "#channelPage > a:nth-last-child(2)", null).end()
                .enbalePageUrlOutput("url")
                .build();

        PageExtract listPage = PageExtract.builder().httpSender()
                .addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
                .addHeader("Accept-Encoding", "gzip, deflate, sdch")
                .addHeader("Accept-Language", "zh-CN,zh;q=0.8")
                .addHeader("Cookie", "fvlid=1493109044271mQd2SHqO; sessionip=36.110.73.210; sessionid=6C536D37-02C1-463A-B5FF-C76E17C4AFAC%7C%7C2017-04-25+16%3A30%3A50.191%7C%7Cwww.baidu.com; ahpau=1; mallsfvi=1493367299924jBMxRTcd%7Cwww.autohome.com.cn%7C103414; ASP.NET_SessionId=wjwfdovfcncyqzzgooxpqoqw; ahpvno=88; sessionuid=6C536D37-02C1-463A-B5FF-C76E17C4AFAC%7C%7C2017-04-25+16%3A30%3A50.191%7C%7Cwww.baidu.com; __utma=1.1033022094.1493109047.1493690212.1493705579.5; __utmb=1.0.10.1493705579; __utmc=1; __utmz=1.1493690212.4.3.utmcsr=sogou|utmccn=(organic)|utmcmd=organic|utmctr=%E6%B1%BD%E8%BD%A6%E4%B9%8B%E5%AE%B6; ref=www.sogou.com%7C%E6%B1%BD%E8%BD%A6%E4%B9%8B%E5%AE%B6%7C0%7Cwww.baidu.com%7C2017-05-02+14%3A46%3A09.198%7C2017-05-02+09%3A55%3A56.690; sessionvid=A10D1092-3CF1-4666-9EE8-221E34FF51D6; area=110199; ahrlid=1493707620950mklO78ZM-1493707640765")
                .addHeader("Upgrade-Insecure-Requests", "1").end().htmlFetcher()
                .configMultiValueColumn("url", "ul.article > li > a", "href").end()
                .build();

        PageExtract resultPage = PageExtract.builder().httpSender()
                .addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
                .addHeader("Accept-Encoding", "gzip, deflate, sdch")
                .addHeader("Accept-Language", "zh-CN,zh;q=0.8")
                .addHeader("Cache-Control", "max-age=0")
                .addHeader("Cookie", "fvlid=1493109044271mQd2SHqO; sessionip=36.110.73.210; sessionid=6C536D37-02C1-463A-B5FF-C76E17C4AFAC%7C%7C2017-04-25+16%3A30%3A50.191%7C%7Cwww.baidu.com; ahpau=1; mallsfvi=1493367299924jBMxRTcd%7Cwww.autohome.com.cn%7C103414; ASP.NET_SessionId=wjwfdovfcncyqzzgooxpqoqw; ahpvno=93; __utma=1.1033022094.1493109047.1493690212.1493705579.5; __utmb=1.0.10.1493705579; __utmc=1; __utmz=1.1493690212.4.3.utmcsr=sogou|utmccn=(organic)|utmcmd=organic|utmctr=%E6%B1%BD%E8%BD%A6%E4%B9%8B%E5%AE%B6; sessionuid=6C536D37-02C1-463A-B5FF-C76E17C4AFAC%7C%7C2017-04-25+16%3A30%3A50.191%7C%7Cwww.baidu.com; ref=www.sogou.com%7C%E6%B1%BD%E8%BD%A6%E4%B9%8B%E5%AE%B6%7C0%7Cwww.baidu.com%7C2017-05-02+14%3A52%3A40.281%7C2017-05-02+09%3A55%3A56.690; sessionvid=A10D1092-3CF1-4666-9EE8-221E34FF51D6; area=110199")
                .addHeader("Upgrade-Insecure-Requests", "1").end().htmlFetcher()
                .configSingleValueColumn("title", "#articlewrap > h1", null)
                .configSingleValueColumn("content", "#articleContent", null).end()
                .enbalePageUrlOutput("url")
                .addConstColumnOutput("source", "汽车之家")
                .build();

        String rootUrl = "http://www.autohome.com.cn/beijing/";
        String rootRefer = "https://www.sogou.com/link?url=DSOYnZeCC_rwEN2bEXbjbXolmWN7p4nuB30X9Rx_BO4.&query=%E6%B1%BD%E8%BD%A6%E4%B9%8B%E5%AE%B6";

        PipeLine pipeLine = new PipeLine(new TimeRangeSheduler(), new TimeRangeRule("0700", "2300"));
        FetchResult result = pipeLine.pipe(rootUrl, rootRefer, indexPage, countPage);
        PipeLine.close(indexPage, countPage);

        Storage storage = new HBaseStorage("xuming.car_article");

        List<String> urls = result.select("url");
        List<String> counts = result.select("count");

        for (int i = 0; i < urls.size(); i++) {
            String url = urls.get(i);
            int count = Integer.parseInt(counts.get(i));

            for (int j = 1; j <= count; j++) {
                String tmpUrl = null;
                String tmpRefer = null;
                if (j == 1) {
                    tmpUrl = url.substring(0, url.lastIndexOf("/") + 1);
                    tmpRefer = url.substring(0, url.lastIndexOf("/") + 1);
                } else {
                    tmpUrl = url.substring(0, url.lastIndexOf("/") + 1) + j + "/";
                    tmpRefer = url.substring(0, url.lastIndexOf("/") + 1) + (j-1) + "/";
                }

                pipeLine.pipe(tmpUrl, tmpRefer, storage, listPage, resultPage);
            }
        }

        PipeLine.close(storage, listPage, resultPage);
    }
}
