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
public class TPYCPingceCrawler {

    public static void main(String[] args) throws IOException {
        PageExtract indexPage = PageExtract.builder().httpSender()
                .addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
                .addHeader("Accept-Encoding", "gzip, deflate, sdch")
                .addHeader("Accept-Language", "zh-CN,zh;q=0.8")
                .addHeader("Cache-Control", "max-age=0")
                .addHeader("Cookie", "canWebp=1; locationCity=%u5317%u4EAC; PClocation=%u5317%u4EAC; pcauto_tas=1493710556458; bdshare_firstime=1493710684706; locationddPro=%u5317%u4EAC%u5E02; visitedfid=15001; captcha=e4cc2e59f6da042feedbe1c15bc82115b1-13e0714057863582096336; locationCity3=%u5317%u4EAC; pcLocate=%7B%22proCode%22%3A%22110000%22%2C%22pro%22%3A%22%E5%8C%97%E4%BA%AC%E5%B8%82%E7%9C%81%22%2C%22cityCode%22%3A%22110000%22%2C%22city%22%3A%22%E5%8C%97%E4%BA%AC%E5%B8%82%22%2C%22dataType%22%3A%22user%22%2C%22expires%22%3A1495011051036%7D; pcautoLocate=%7B%22proId%22%3A6%2C%22cityId%22%3A2%2C%22url%22%3A%22http%3A%2F%2Fwww.pcauto.com.cn%2Fqcbj%2Fbj%2F%22%2C%22dataTypeAuto%22%3A%22user%22%7D; divStatus=1; pcsuv=1493710518299.a.263107321; pcuvdata=lastAccessTime=1493716100077|visits=7; channel=6233")
                .addHeader("Upgrade-Insecure-Requests", "1").end().htmlFetcher()
                .configMultiValueColumn("url", "#nav > li:nth-child(2) > a, #nav > li:nth-child(3) > a, #nav > li:nth-child(4) > a", "href").end()
                .build();

        PageExtract countPage = PageExtract.builder().httpSender()
                .addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
                .addHeader("Accept-Encoding", "gzip, deflate, sdch")
                .addHeader("Accept-Language", "zh-CN,zh;q=0.8")
                .addHeader("Cache-Control", "max-age=0")
                .addHeader("Cookie", "canWebp=1; locationCity=%u5317%u4EAC; PClocation=%u5317%u4EAC; pcauto_tas=1493710556458; bdshare_firstime=1493710684706; locationddPro=%u5317%u4EAC%u5E02; visitedfid=15001; captcha=e4cc2e59f6da042feedbe1c15bc82115b1-13e0714057863582096336; locationCity3=%u5317%u4EAC; pcLocate=%7B%22proCode%22%3A%22110000%22%2C%22pro%22%3A%22%E5%8C%97%E4%BA%AC%E5%B8%82%E7%9C%81%22%2C%22cityCode%22%3A%22110000%22%2C%22city%22%3A%22%E5%8C%97%E4%BA%AC%E5%B8%82%22%2C%22dataType%22%3A%22user%22%2C%22expires%22%3A1495011051036%7D; pcautoLocate=%7B%22proId%22%3A6%2C%22cityId%22%3A2%2C%22url%22%3A%22http%3A%2F%2Fwww.pcauto.com.cn%2Fqcbj%2Fbj%2F%22%2C%22dataTypeAuto%22%3A%22user%22%7D; divStatus=1; pcsuv=1493710518299.a.263107321; pcuvdata=lastAccessTime=1493716100077|visits=7; channel=6233")
                .addHeader("Upgrade-Insecure-Requests", "1").end().htmlFetcher()
                .configSingleValueColumn("count", "div.layA > div > div.pcauto_page > a:nth-last-child(2)", null).end()
                .enbalePageUrlOutput("url")
                .build();

        PageExtract listPage = PageExtract.builder().httpSender()
                .addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
                .addHeader("Accept-Encoding", "gzip, deflate, sdch")
                .addHeader("Accept-Language", "zh-CN,zh;q=0.8")
                .addHeader("Cache-Control", "max-age=0")
                .addHeader("Cookie", "canWebp=1; locationCity=%u5317%u4EAC; PClocation=%u5317%u4EAC; pcauto_tas=1493710556458; bdshare_firstime=1493710684706; locationddPro=%u5317%u4EAC%u5E02; visitedfid=15001; captcha=e4cc2e59f6da042feedbe1c15bc82115b1-13e0714057863582096336; locationCity3=%u5317%u4EAC; pcLocate=%7B%22proCode%22%3A%22110000%22%2C%22pro%22%3A%22%E5%8C%97%E4%BA%AC%E5%B8%82%E7%9C%81%22%2C%22cityCode%22%3A%22110000%22%2C%22city%22%3A%22%E5%8C%97%E4%BA%AC%E5%B8%82%22%2C%22dataType%22%3A%22user%22%2C%22expires%22%3A1495011051036%7D; pcautoLocate=%7B%22proId%22%3A6%2C%22cityId%22%3A2%2C%22url%22%3A%22http%3A%2F%2Fwww.pcauto.com.cn%2Fqcbj%2Fbj%2F%22%2C%22dataTypeAuto%22%3A%22user%22%7D; divStatus=1; pcsuv=1493710518299.a.263107321; pcuvdata=lastAccessTime=1493716100077|visits=7; channel=6233")
                .addHeader("Upgrade-Insecure-Requests", "1").end().htmlFetcher()
                .configMultiValueColumn("url", "div.list > div.box-bd > div.pic-txt > div.txt > p.tit > a", "href").end()
                .build();

        PageExtract resultPage = PageExtract.builder().httpSender()
                .setCharset("gbk")
                .addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
                .addHeader("Accept-Encoding", "gzip, deflate, sdch")
                .addHeader("Accept-Language", "zh-CN,zh;q=0.8")
                .addHeader("Cache-Control", "max-age=0")
                .addHeader("Cookie", "canWebp=1; locationCity=%u5317%u4EAC; PClocation=%u5317%u4EAC; pcauto_tas=1493710556458; bdshare_firstime=1493710684706; locationddPro=%u5317%u4EAC%u5E02; visitedfid=15001; captcha=e4cc2e59f6da042feedbe1c15bc82115b1-13e0714057863582096336; locationCity3=%u5317%u4EAC; divStatus=1; pcsuv=1493710518299.a.263107321; pcuvdata=lastAccessTime=1493716221220|visits=7; channel=6233; pcLocate=%7B%22proCode%22%3A%22110000%22%2C%22pro%22%3A%22%E5%8C%97%E4%BA%AC%E5%B8%82%E7%9C%81%22%2C%22cityCode%22%3A%22110000%22%2C%22city%22%3A%22%E5%8C%97%E4%BA%AC%E5%B8%82%22%2C%22dataType%22%3A%22user%22%2C%22expires%22%3A1495012276740%7D; pcautoLocate=%7B%22proId%22%3A6%2C%22cityId%22%3A2%2C%22url%22%3A%22http%3A%2F%2Fwww.pcauto.com.cn%2Fqcbj%2Fbj%2F%22%2C%22dataTypeAuto%22%3A%22user%22%7D")
                .addHeader("Upgrade-Insecure-Requests", "1").end().htmlFetcher()
                .configSingleValueColumn("title", "div.mainBox > div.divArt > div.artTop > h1.artTit", null)
                .configSingleValueColumn("content", "div.mainBox > div.divArt > div.artText", null)
                .configValueProcessor("title", new Processor() {
                    @Override
                    public String process(String value, String url) {
                        String[] tokens = value.split("  ", -1);
                        if (tokens.length == 2) {
                            return tokens[1];
                        } else {
                            return tokens[0];
                        }
                    }
                }).end()
                .enbalePageUrlOutput("url")
                .addConstColumnOutput("source", "太平洋汽车网")
                .build();

        PipeLine pipeLine = new PipeLine(new TimeRangeSheduler(), new TimeRangeRule("0700", "2300"));

        String rootUrl = "http://www.pcauto.com.cn/pingce/";
        String rootRefer = "http://www.pcauto.com.cn/";
        FetchResult result = pipeLine.pipe(rootUrl, rootRefer, indexPage, countPage);
        PipeLine.close(indexPage, countPage);

        Storage storage = new HBaseStorage("xuming.car_article");

        List<String> urls = result.select("url");
        List<String> counts = result.select("count");

        for (int i = 0; i < urls.size(); i++) {
            String url = urls.get(i);
            int count = Integer.parseInt(counts.get(i));

            String tplUrl = url + "%s";
            String tplRefer = url + "%s";
            for (int j = 1; j <= count; j++) {
                String realUrl = null;
                String realRefer = null;
                if (j == 1) {
                    realUrl = String.format(tplUrl, "");
                    realRefer = String.format(tplRefer, "");
                } else {
                    realUrl = String.format(tplUrl, "index_" + (j-1) + ".html");
                    realRefer = String.format(tplRefer, "index_" + ((j-2)==0?"":(j-2)) + ".html");
                }

                pipeLine.pipe(realUrl, realRefer, storage, listPage, resultPage);
            }
        }

        PipeLine.close(storage, listPage, resultPage);
    }
}
