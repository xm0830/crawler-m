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
 * Created by xuming on 2017/5/2.
 */
public class CHKoubeiCrawler {

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
                        return "http://k.autohome.com.cn" + value;
                    }
                }).end()
                .build();

        PageExtract urlAndCountPage = PageExtract.builder().httpSender()
                .addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
                .addHeader("Accept-Encoding", "gzip, deflate, sdch")
                .addHeader("Accept-Language", "zh-CN,zh;q=0.8")
                .addHeader("Cookie", "fvlid=1493109044271mQd2SHqO; sessionip=36.110.73.210; sessionid=6C536D37-02C1-463A-B5FF-C76E17C4AFAC%7C%7C2017-04-25+16%3A30%3A50.191%7C%7Cwww.baidu.com; ahpau=1; bdshare_firstime=1493109090091; guidance=true; mallsfvi=1493367299924jBMxRTcd%7Cwww.autohome.com.cn%7C103414; sessionuid=6C536D37-02C1-463A-B5FF-C76E17C4AFAC%7C%7C2017-04-25+16%3A30%3A50.191%7C%7Cwww.baidu.com; __utma=1.1033022094.1493109047.1493379093.1493690212.4; __utmc=1; __utmz=1.1493690212.4.3.utmcsr=sogou|utmccn=(organic)|utmcmd=organic|utmctr=%E6%B1%BD%E8%BD%A6%E4%B9%8B%E5%AE%B6; ASP.NET_SessionId=qjuhyusfscqswxov3r2kgo42; ahpvno=68; ref=www.sogou.com%7C%E6%B1%BD%E8%BD%A6%E4%B9%8B%E5%AE%B6%7C0%7Cwww.baidu.com%7C2017-05-02+13%3A05%3A35.454%7C2017-05-02+09%3A55%3A56.690; sessionvid=66F204C9-3E8E-44AE-8FD8-8A08F8D19098; area=110199; ahrlid=1493701586744DU84PETr-1493701710011")
                .addHeader("Upgrade-Insecure-Requests", "1").end().htmlFetcher()
                .configSingleValueColumn("count", "#maodian > div > div > div.mouth-cont > div.screening > div > div.tab-nav > span", null)
                .configValueProcessor("count", new Processor() {
                    @Override
                    public String process(String value, String url) {
                        return value.substring(2, value.length() - 3);
                    }
                }).end()
                .enbalePageUrlOutput("url")
                .build();

        PageExtract listPage = PageExtract.builder().httpSender()
                .addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
                .addHeader("Accept-Encoding", "gzip, deflate, sdch")
                .addHeader("Accept-Language", "zh-CN,zh;q=0.8")
                .addHeader("Cookie", "fvlid=1493109044271mQd2SHqO; sessionip=36.110.73.210; sessionid=6C536D37-02C1-463A-B5FF-C76E17C4AFAC%7C%7C2017-04-25+16%3A30%3A50.191%7C%7Cwww.baidu.com; ahpau=1; bdshare_firstime=1493109090091; guidance=true; mallsfvi=1493367299924jBMxRTcd%7Cwww.autohome.com.cn%7C103414; sessionuid=6C536D37-02C1-463A-B5FF-C76E17C4AFAC%7C%7C2017-04-25+16%3A30%3A50.191%7C%7Cwww.baidu.com; __utma=1.1033022094.1493109047.1493379093.1493690212.4; __utmc=1; __utmz=1.1493690212.4.3.utmcsr=sogou|utmccn=(organic)|utmcmd=organic|utmctr=%E6%B1%BD%E8%BD%A6%E4%B9%8B%E5%AE%B6; ASP.NET_SessionId=qjuhyusfscqswxov3r2kgo42; ahpvno=68; ref=www.sogou.com%7C%E6%B1%BD%E8%BD%A6%E4%B9%8B%E5%AE%B6%7C0%7Cwww.baidu.com%7C2017-05-02+13%3A05%3A35.454%7C2017-05-02+09%3A55%3A56.690; sessionvid=66F204C9-3E8E-44AE-8FD8-8A08F8D19098; area=110199; ahrlid=1493701586744DU84PETr-1493701710011")
                .addHeader("Upgrade-Insecure-Requests", "1").end().htmlFetcher()
                .configMultiValueColumn("url", "div.mouthcon div.mouth-main > div.mouth-item > div.cont-title > div > a", "href").end()
                .build();

        PageExtract resultPage = PageExtract.builder().httpSender()
                .addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
                .addHeader("Accept-Encoding", "gzip, deflate, sdch")
                .addHeader("Accept-Language", "zh-CN,zh;q=0.8")
                .addHeader("Cache-Control", "max-age=0")
                .addHeader("Cookie", "fvlid=1493109044271mQd2SHqO; sessionip=36.110.73.210; sessionid=6C536D37-02C1-463A-B5FF-C76E17C4AFAC%7C%7C2017-04-25+16%3A30%3A50.191%7C%7Cwww.baidu.com; ahpau=1; bdshare_firstime=1493109090091; guidance=true; mallsfvi=1493367299924jBMxRTcd%7Cwww.autohome.com.cn%7C103414; sessionuid=6C536D37-02C1-463A-B5FF-C76E17C4AFAC%7C%7C2017-04-25+16%3A30%3A50.191%7C%7Cwww.baidu.com; __utma=1.1033022094.1493109047.1493379093.1493690212.4; __utmc=1; __utmz=1.1493690212.4.3.utmcsr=sogou|utmccn=(organic)|utmcmd=organic|utmctr=%E6%B1%BD%E8%BD%A6%E4%B9%8B%E5%AE%B6; ASP.NET_SessionId=qjuhyusfscqswxov3r2kgo42; ahpvno=70; ref=www.sogou.com%7C%E6%B1%BD%E8%BD%A6%E4%B9%8B%E5%AE%B6%7C0%7Cwww.baidu.com%7C2017-05-02+14%3A00%3A38.366%7C2017-05-02+09%3A55%3A56.690; sessionvid=A10D1092-3CF1-4666-9EE8-221E34FF51D6; area=110199")
                .addHeader("Upgrade-Insecure-Requests", "1").end().htmlFetcher()
                .configSingleValueColumn("title", "div.content > div.subnav > div.subnav-title > div > a", null)
                .configSingleValueColumn("content", "div.content > div:nth-child(3) > div > div > div.mouth-cont > div > div > div.mouthcon-cont-right > div.mouth-main > div:nth-last-child(1) > div.text-con", null).end()
                .enbalePageUrlOutput("url")
                .addConstColumnOutput("source", "汽车之家")
                .build();

        String rootUrl = "http://k.autohome.com.cn/suva1/";
        String rootRefer = "http://k.autohome.com.cn/";
        PipeLine pipeLine = new PipeLine(new TimeRangeSheduler(), new TimeRangeRule("0700", "2300"));
        FetchResult result = pipeLine.pipe(rootUrl, rootRefer, categoryPage, carPage, urlAndCountPage);

        PipeLine.close(categoryPage, carPage, urlAndCountPage);

        Storage storage = new HBaseStorage("xuming.car_koubei");

        List<String> listUrls = result.select("url");
        List<String> listCounts = result.select("count");

        for (int i = 0; i < listUrls.size(); i++) {
            String tmpUrl = listUrls.get(i);
            int tmpCount = Integer.parseInt(listCounts.get(i));

            int pageSize = 15;
            int pageCount = (tmpCount + pageSize - 1)/ pageSize;
            String tplUrl = tmpUrl + "%s";
            String tplRefer = tmpUrl + "%s";
            for (int j = 1; j <= pageCount; j++) {
                String realUrl = null;
                String realRefer = null;
                if (j == 1) {
                    realUrl = String.format(tplUrl, "");
                    realRefer = String.format(tplRefer, "");
                } else {
                    realUrl = String.format(tplUrl, "index_" + j + ".html");
                    realRefer = String.format(tplRefer, "index_" + (j-1) + ".html");
                }

                pipeLine.pipe(realUrl, realRefer, storage, listPage, resultPage);
            }
        }

        PipeLine.close(storage, listPage, resultPage);
    }
}
