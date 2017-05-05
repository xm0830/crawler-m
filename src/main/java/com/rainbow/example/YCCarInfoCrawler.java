package com.rainbow.example;

import com.rainbow.api.PageExtract;
import com.rainbow.api.PipeLine;
import com.rainbow.fetcher.JsonFetcher;
import com.rainbow.fetcher.Processor;
import com.rainbow.storage.HBaseStorage;
import com.rainbow.storage.Storage;

import java.io.IOException;

/**
 * Created by xuming on 2017/4/27.
 */
public class YCCarInfoCrawler {
    public static void main(String[] args) throws IOException {
        PageExtract brandList = PageExtract.builder().httpSender()
                .addHeader("Accept", "*/*")
                .addHeader("Accept-Encoding", "gzip, deflate, sdch")
                .addHeader("Accept-Language", "zh-CN,zh;q=0.8")
                .addHeader("Cache-Control", "max-age=0")
                .addHeader("Cookie", "locatecity=110100; bitauto_ipregion=36.110.73.210%3a%e5%8c%97%e4%ba%ac%e5%b8%82%e5%8c%97%e4%ba%ac%e5%b8%82%3b201%2c%e5%8c%97%e4%ba%ac%2cbeijing")
                .addHeader("Upgrade-Insecure-Requests", "1").end().jsonFetcher()
                .configRootType(JsonFetcher.JSONType.OBJECT)
                .configSelector("url", "brand", JsonFetcher.JSONType.OBJECT)
                .configSelector("url", "[A-Z]", JsonFetcher.JSONType.ARRAY)
                .configSelector("url", "url", JsonFetcher.JSONType.OBJECT)
                .configProcessor("url", new Processor() {
                    @Override
                    public String process(String value, String url) {
                        return "http://car.bitauto.com/" + value;
                    }
                }).end()
                .build();

        PageExtract chexingList = PageExtract.builder().httpSender()
                .addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
                .addHeader("Accept-Encoding", "gzip, deflate, sdch")
                .addHeader("Accept-Language", "zh-CN,zh;q=0.8")
                .addHeader("Cookie", "XCWEBLOG_testcookie=yes; locatecity=110100; XCWEBLOG_testcookie=yes; _dc3c=1; _dcisnw=1; dmt10=2%7C0%7C0%7Ccar.bitauto.com%2F%2Ftree_chexing%2Fmb_9%2F%7C; dmts10=1; dm10=1%7C1493272012%7C0%7C%7C%7C%7C%7C1493271475%7C1493271475%7C0%7C1493271475%7Cfc6c1e2cfecb71ae12d281042eb1137c%7C0%7C%7C; dcad10=; dc_search10=; CIGDCID=fc6c1e2cfecb71ae12d281042eb1137c; bitauto_ipregion=36.110.73.210%3a%e5%8c%97%e4%ba%ac%e5%b8%82%e5%8c%97%e4%ba%ac%e5%b8%82%3b201%2c%e5%8c%97%e4%ba%ac%2cbeijing; Hm_lvt_7b86db06beda666182190f07e1af98e3=1493031766,1493265453,1493272012; Hm_lpvt_7b86db06beda666182190f07e1af98e3=1493272012")
                .addHeader("Upgrade-Insecure-Requests", "1").end().htmlFetcher()
                .configMultiValueColumn("url", "#divCsLevel_0 > div ul.p-list > li:nth-child(1) > a", "href")
                .configValueProcessor("url", new Processor() {
                    @Override
                    public String process(String value, String url) {
                        return "http://car.bitauto.com" + value + "pingce";
                    }
                }).end()
                .build();

        PageExtract chexingPingceUrls = PageExtract.builder().httpSender()
                .addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
                .addHeader("Accept-Encoding", "gzip, deflate, sdch")
                .addHeader("Accept-Language", "zh-CN,zh;q=0.8")
                .addHeader("Cookie", "XCWEBLOG_testcookie=yes; XCWEBLOG_testcookie=yes; locatecity=110100; CarStateForBitAuto=d7352bf5-36ba-eecd-ef9b-cdbbbcbe0543; BitAutoUserCode=2bf9c54d-c19f-a7c2-80ca-110f6a441cb9; UserGuid=ec368689-e53c-4242-907d-c932d5deb115; _dc3c=1; _dcisnw=1; bitauto_ipregion=36.110.73.210%3a%e5%8c%97%e4%ba%ac%e5%b8%82%e5%8c%97%e4%ba%ac%e5%b8%82%3b201%2c%e5%8c%97%e4%ba%ac%2cbeijing; dmt10=4%7C0%7C0%7Ccar.bitauto.com%2Faerfaluomiougiulia%2Fpingce%2F%7C; dmts10=1; dm10=1%7C1493278290%7C0%7C%7C%7C%7C%7C1493277576%7C1493277576%7C0%7C1493277576%7Cfc6c1e2cfecb71ae12d281042eb1137c%7C0%7C%7C; dcad10=; dc_search10=; CIGDCID=fc6c1e2cfecb71ae12d281042eb1137c")
                .addHeader("If-Modified-Since", "Thu, 27 Apr 2017 07:10:39 GMT")
                .addHeader("Upgrade-Insecure-Requests", "1").end().htmlFetcher()
                .configMultiValueColumn("url", "#leftUl > li > a, #leftUl > li:nth-child(1)", "href")
                .configValueProcessor("url", new Processor() {
                    @Override
                    public String process(String value, String url) {
                        if (value == null || "".equals(value)) {
                            return "http://car.bitauto.com/aerfaluomiougiulia/pingce/1/";
                        } else {
                            return "http://car.bitauto.com" + value;
                        }
                    }
                }).end()
                .build();

        PageExtract resultPage = PageExtract.builder().httpSender()
                .addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
                .addHeader("Accept-Encoding", "gzip, deflate, sdch")
                .addHeader("Accept-Language", "zh-CN,zh;q=0.8")
                .addHeader("Cookie", "XCWEBLOG_testcookie=yes; XCWEBLOG_testcookie=yes; locatecity=110100; CarStateForBitAuto=d7352bf5-36ba-eecd-ef9b-cdbbbcbe0543; BitAutoUserCode=2bf9c54d-c19f-a7c2-80ca-110f6a441cb9; UserGuid=ec368689-e53c-4242-907d-c932d5deb115; _dc3c=1; _dcisnw=1; bitauto_ipregion=36.110.73.210%3a%e5%8c%97%e4%ba%ac%e5%b8%82%e5%8c%97%e4%ba%ac%e5%b8%82%3b201%2c%e5%8c%97%e4%ba%ac%2cbeijing; dmt10=4%7C0%7C0%7Ccar.bitauto.com%2Faerfaluomiougiulia%2Fpingce%2F%7C; dmts10=1; dm10=1%7C1493278290%7C0%7C%7C%7C%7C%7C1493277576%7C1493277576%7C0%7C1493277576%7Cfc6c1e2cfecb71ae12d281042eb1137c%7C0%7C%7C; dcad10=; dc_search10=; CIGDCID=fc6c1e2cfecb71ae12d281042eb1137c")
                .addHeader("If-Modified-Since", "Thu, 27 Apr 2017 07:10:39 GMT")
                .addHeader("Upgrade-Insecure-Requests", "1").end().htmlFetcher()
                .configSingleValueColumn("title", "article>h1[class='tit-h1']", null)
                .configSingleValueColumn("content", "article>div[class='article-content']", null).end()
                .enbalePageUrlOutput("url")
                .addConstColumnOutput("source", "易车网")
                .build();

        Storage storage = new HBaseStorage("xuming.car_article");

        String url = "http://api.car.bitauto.com/CarInfo/getlefttreejson.ashx?tagtype=chexing&pagetype=masterbrand&objid=0";
        String refer = "http://car.bitauto.com/";
        new PipeLine().pipe(url, refer, storage, brandList, chexingList, chexingPingceUrls, resultPage);
        PipeLine.close(storage, brandList, chexingList, chexingPingceUrls, resultPage);
    }
}
