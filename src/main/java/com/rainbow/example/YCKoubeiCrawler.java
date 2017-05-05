package com.rainbow.example;

import com.rainbow.api.PageExtract;
import com.rainbow.api.PipeLine;
import com.rainbow.fetcher.FetchResult;
import com.rainbow.fetcher.JsonFetcher;
import com.rainbow.fetcher.Processor;
import com.rainbow.storage.HBaseStorage;
import com.rainbow.storage.Storage;

import java.io.IOException;
import java.util.List;

/**
 * Created by xuming on 2017/4/27.
 */
public class YCKoubeiCrawler {
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
                .configSelector("url", "[A]", JsonFetcher.JSONType.ARRAY)
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
                        return "http://car.bitauto.com" + value + "koubei/";
                    }
                }).end()
                .build();

        PageExtract chexingKoubeiUrls = PageExtract.builder().httpSender()
                .addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
                .addHeader("Accept-Encoding", "gzip, deflate, sdch")
                .addHeader("Accept-Language", "zh-CN,zh;q=0.8")
                .addHeader("Cache-Control", "max-age=0")
                .addHeader("Cookie", "XCWEBLOG_testcookie=yes; XCWEBLOG_testcookie=yes; XCWEBLOG_testcookie=yes; locatecity=110100; CarStateForBitAuto=d7352bf5-36ba-eecd-ef9b-cdbbbcbe0543; BitAutoUserCode=2bf9c54d-c19f-a7c2-80ca-110f6a441cb9; UserGuid=ec368689-e53c-4242-907d-c932d5deb115; _dcisnw=1; BitAutoLogId=7f8401f52f700bb5fbf0ecd6b621e7d0; doubleADCookie=%2C2931%3A0%2C2922%3A0%2C2926%3A0%2C3181%3A0%2C3467%3A0%2C3087%3A0%2C4456%3A0%2C4455%3A0%2C4454%3A0%2C3443%3A0%2C4430%3A0%2C4438%3A1%2C4428%3A0%2C4608%3A0; ASP.NET_SessionId=m10swpat4crmekb42qfmgprj; _dc3c=1; XCWEBLOG_testcookie=yes; Hm_lvt_7b86db06beda666182190f07e1af98e3=1493031766,1493265453,1493272012; Hm_lpvt_7b86db06beda666182190f07e1af98e3=1493286998; csids=3023_4277_4175_2872_4117_2412; bitauto_ipregion=36.110.73.210%3a%e5%8c%97%e4%ba%ac%e5%b8%82%3b201%2c%e5%8c%97%e4%ba%ac%2cbeijing; Hm_lvt_96d6ae57edf658c0e19e6529cc4fc694=1493108943; Hm_lpvt_96d6ae57edf658c0e19e6529cc4fc694=1493287403; dmt10=22%7C0%7C0%7Ccar.bitauto.com%2Ftengyic30%2Fkoubei%2Fgengduo%2F3023-0-0-0-0-0-0-0-0-0-0--1-10.html%7Ccar.bitauto.com%2Ftengyic30%2Fkoubei%2F; dmts10=1; dm10=2%7C1493287403%7C0%7C%7C%7C%7C%7C1493277576%7C1493277576%7C1493277576%7C1493286110%7Cfc6c1e2cfecb71ae12d281042eb1137c%7C0%7C%7C; dcad10=; dc_search10=; CIGDCID=fc6c1e2cfecb71ae12d281042eb1137c")
                .addHeader("Upgrade-Insecure-Requests", "1").end().htmlFetcher()
                .configSingleValueColumn("url", "#hidListUrl", "value")
                .configSingleValueColumn("count", "#hidListCount", "value")
                .configValueProcessor("url", new Processor() {
                    @Override
                    public String process(String value, String url) {
                        if (value == null || value.equals("")) {
                            return url;
                        }
                        return value;
                    }
                }).end()
                .enbalePageUrlOutput("refer")
                .build();

        PageExtract chexingKoubeiDetailUrls = PageExtract.builder().httpSender()
                .addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
                .addHeader("Accept-Encoding", "gzip, deflate, sdch")
                .addHeader("Accept-Language", "zh-CN,zh;q=0.8")
                .addHeader("Cache-Control", "max-age=0")
                .addHeader("Cookie", "XCWEBLOG_testcookie=yes; XCWEBLOG_testcookie=yes; XCWEBLOG_testcookie=yes; locatecity=110100; CarStateForBitAuto=d7352bf5-36ba-eecd-ef9b-cdbbbcbe0543; BitAutoUserCode=2bf9c54d-c19f-a7c2-80ca-110f6a441cb9; UserGuid=ec368689-e53c-4242-907d-c932d5deb115; _dcisnw=1; BitAutoLogId=7f8401f52f700bb5fbf0ecd6b621e7d0; doubleADCookie=%2C2931%3A0%2C2922%3A0%2C2926%3A0%2C3181%3A0%2C3467%3A0%2C3087%3A0%2C4456%3A0%2C4455%3A0%2C4454%3A0%2C3443%3A0%2C4430%3A0%2C4438%3A1%2C4428%3A0%2C4608%3A0; ASP.NET_SessionId=m10swpat4crmekb42qfmgprj; _dc3c=1; XCWEBLOG_testcookie=yes; Hm_lvt_7b86db06beda666182190f07e1af98e3=1493031766,1493265453,1493272012; Hm_lpvt_7b86db06beda666182190f07e1af98e3=1493286998; csids=3023_4277_4175_2872_4117_2412; bitauto_ipregion=36.110.73.210%3a%e5%8c%97%e4%ba%ac%e5%b8%82%3b201%2c%e5%8c%97%e4%ba%ac%2cbeijing; Hm_lvt_96d6ae57edf658c0e19e6529cc4fc694=1493108943; Hm_lpvt_96d6ae57edf658c0e19e6529cc4fc694=1493287403; dmt10=22%7C0%7C0%7Ccar.bitauto.com%2Ftengyic30%2Fkoubei%2Fgengduo%2F3023-0-0-0-0-0-0-0-0-0-0--1-10.html%7Ccar.bitauto.com%2Ftengyic30%2Fkoubei%2F; dmts10=1; dm10=2%7C1493287403%7C0%7C%7C%7C%7C%7C1493277576%7C1493277576%7C1493277576%7C1493286110%7Cfc6c1e2cfecb71ae12d281042eb1137c%7C0%7C%7C; dcad10=; dc_search10=; CIGDCID=fc6c1e2cfecb71ae12d281042eb1137c")
                .addHeader("Upgrade-Insecure-Requests", "1").end().htmlFetcher()
                .configMultiValueColumn("url", "div[class='kb-list-box'] > div[class='cont-box'] > div[class='main'] > p > a", "href").end()
                .build();

        PageExtract resultPage = PageExtract.builder().httpSender()
                .addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
                .addHeader("Accept-Encoding", "gzip, deflate, sdch")
                .addHeader("Accept-Language", "zh-CN,zh;q=0.8")
                .addHeader("Cache-Control", "max-age=0")
                .addHeader("Cookie", "XCWEBLOG_testcookie=yes; XCWEBLOG_testcookie=yes; XCWEBLOG_testcookie=yes; locatecity=110100; CarStateForBitAuto=d7352bf5-36ba-eecd-ef9b-cdbbbcbe0543; BitAutoUserCode=2bf9c54d-c19f-a7c2-80ca-110f6a441cb9; UserGuid=ec368689-e53c-4242-907d-c932d5deb115; _dcisnw=1; BitAutoLogId=7f8401f52f700bb5fbf0ecd6b621e7d0; doubleADCookie=%2C2931%3A0%2C2922%3A0%2C2926%3A0%2C3181%3A0%2C3467%3A0%2C3087%3A0%2C4456%3A0%2C4455%3A0%2C4454%3A0%2C3443%3A0%2C4430%3A0%2C4438%3A1%2C4428%3A0%2C4608%3A0; ASP.NET_SessionId=m10swpat4crmekb42qfmgprj; _dc3c=1; XCWEBLOG_testcookie=yes; csids=2593_4794_3023_4277_4175_2872; Hm_lvt_7b86db06beda666182190f07e1af98e3=1493031766,1493265453,1493272012; Hm_lpvt_7b86db06beda666182190f07e1af98e3=1493289922; bitauto_ipregion=36.110.73.210%3a%e5%8c%97%e4%ba%ac%e5%b8%82%e5%8c%97%e4%ba%ac%e5%b8%82%3b201%2c%e5%8c%97%e4%ba%ac%2cbeijing; Hm_lvt_96d6ae57edf658c0e19e6529cc4fc694=1493108943; Hm_lpvt_96d6ae57edf658c0e19e6529cc4fc694=1493291473; dmt10=35%7C0%7C0%7Ccar.bitauto.com%2Ftengyic30%2Fkoubei%2F812462%2F%7Ccar.bitauto.com%2Ftengyic30%2Fkoubei%2Fgengduo%2F3023-0-0-0-0-0-0-0-0-0-0--1-10.html; dmts10=1; dm10=2%7C1493291473%7C0%7C%7C%7C%7C%7C1493277576%7C1493277576%7C1493277576%7C1493286110%7Cfc6c1e2cfecb71ae12d281042eb1137c%7C0%7C%7C; dcad10=; dc_search10=; CIGDCID=fc6c1e2cfecb71ae12d281042eb1137c")
                .addHeader("Upgrade-Insecure-Requests", "1").end().htmlFetcher()
                .configSingleValueColumn("title", "div[class='kb-section-main'] > div:nth-child(1) > div[class='con-l'] > h6", null)
                .configSingleValueColumn("content", "#div_ImgLoadArea", null).end()
                .enbalePageUrlOutput("url")
                .addConstColumnOutput("source", "易车网")
                .build();


        String url = "http://api.car.bitauto.com/CarInfo/getlefttreejson.ashx?tagtype=chexing&pagetype=masterbrand&objid=0";
        String refer = "http://car.bitauto.com/";
        FetchResult result = new PipeLine().pipe(url, refer, brandList, chexingList, chexingKoubeiUrls);
        PipeLine.close(null, brandList, chexingList, chexingKoubeiUrls);

        List<String> refer1 = result.select("refer");
        List<String> url1 = result.select("url");
        List<String> count1 = result.select("count");

        Storage storage = new HBaseStorage("xuming.car_koubei");

        for (int i = 0; i < url1.size(); i++) {
            String tmpUrl = url1.get(i);
            int tmpCount = Integer.parseInt(count1.get(i));

            if (tmpUrl != null && !"".equals(tmpUrl)) {
                int pageSize = getPageSize(tmpUrl, tmpCount);
                int pageCount = (tmpCount + pageSize - 1)/pageSize;
                for (int j = 1; j <= pageCount; j++) {
                    new PipeLine().pipe(urlGen(tmpUrl, j), refer1.get(i), storage, chexingKoubeiDetailUrls, resultPage);
                }
            }
        }

        PipeLine.close(storage, chexingKoubeiDetailUrls, resultPage);
    }

    public static String urlGen(String url, int index) {
        int i = url.lastIndexOf("-");
        if (i != -1) {
            String part1 = url.substring(0, i-1);
            String part2 = url.substring(i);

            return part1 + index + part2;
        } else {
            return url;
        }
    }

    public static int getPageSize(String url, int count) {
        if (url != null) {
            int i = url.lastIndexOf("-");
            if (i != -1) {
                return Integer.parseInt(url.substring(i + 1, url.lastIndexOf(".")));
            } else {
                return count;
            }
        }

        return -1;
    }
}
