package com.pxene.example;

import com.pxene.api.PageExtract;
import com.pxene.api.PipeLine;
import com.pxene.fetcher.JsonFetcher;
import com.pxene.storage.HBaseStorage;
import com.pxene.storage.Storage;
import org.apache.log4j.PropertyConfigurator;

import java.io.IOException;

/**
 * Created by xuming on 2017/4/26.
 */
public class YCArticleCrawler {

    public static void main(String[] args) throws IOException, InterruptedException {
        PropertyConfigurator.configure(YCArticleCrawler.class.getResource("/log4j.properties"));

        PageExtract page1 = PageExtract.builder().httpSender()
                .addHeader("Accept", "text/javascript, application/javascript, application/ecmascript, application/x-ecmascript, */*; q=0.01")
                .addHeader("Accept-Language", "zh-CN,zh;q=0.8")
                .addHeader("Cookie", "XCWEBLOG_testcookie=yes; ASP.NET_SessionId=zibmtvy1zksr50ehftkr3dj3; locatecity=110100; UserGuid=2aaea016-ac76-4b92-bf3b-e3c8028582c7; BitAutoLogId=479ad4dfe4777813f93c1a6b45cfd3ea; _dadtpqcuq=2b03c51d4a1cae5c4ae5bf5be98cc6672c429089; CarStateForBitAuto=ae591959-2d06-1c0d-1661-d9e830ee1521; BitAutoUserCode=dbb97454-5f9c-d7b6-035d-884ad6645be8; doubleADCookie=%2C2931%3A0%2C2922%3A0%2C2926%3A1%2C3181%3A0%2C3467%3A0%2C3087%3A0%2C4456%3A0%2C4455%3A0%2C4454%3A0%2C3443%3A0%2C4430%3A0%2C4438%3A1%2C4428%3A0%2C4608%3A0%2C3595%3A0; _dc3c=1; Hm_lvt_ae6bb7b1c82384836a5be7f1c3faed65=1492687299,1493031723; Hm_lpvt_ae6bb7b1c82384836a5be7f1c3faed65=1493031723; csids=1879_2045_1661_4616; dmt10=10%7C1%7C0%7Cnews.bitauto.com%2Fothers%2F20160401%2F1906578266-3.html%7Cwww.bitauto.com%2Fpingce%2F%3Fpage%3D2; dm10=8%7C1493032279%7C2%7Cwww.baidu.com%7C%2Fs%3Fie%3Dutf-8%26f%3D3%26rsv_bp%3D1%26rsv_idx%3D2%26tn%3Dbaiduhome_pg%26wd%3D%25E6%2598%2593%25E8%25BD%25A6%25E7%25BD%2591%26rsv_spt%3D1%26oq%3D%2525E6%252594%2525AF%2525E6%25258C%252581ajax%2525E8%2525AF%2525B7%2525E6%2525B1%252582%2525E7%25259A%252584%2525E7%252588%2525AC%2525E8%252599%2525AB%2525E6%2525A1%252586%2525E6%25259E%2525B6%26rsv_pq%3Db221b861000034c8%26rsv_t%3D744ccxgECWdFFy6kHuMK4zMJYu%252BPZ7AvLefF9IsiELbsSX%252FN4KA1S%252BYiwrVy3rbxHZ6t%26rqlang%3Dcn%26rsv_enter%3D1%26inputT%3D402882%26rsv_sug3%3D46%26bs%3D%25E6%2594%25AF%25E6%258C%2581ajax%25E8%25AF%25B7%25E6%25B1%2582%25E7%259A%2584%25E7%2588%25AC%25E8%2599%25AB%25E6%25A1%2586%25E6%259E%25B6%7C%7C%7C1492682857%7C1493030244%7C1493026391%7C1493030244%7Cfc6c1e2cfecb71ae12d281042eb1137c%7C0%7C%7C; dcad10=utm_campaign%3A201704_4008%7Cutm_source%3Abitauto%7Cutm_medium%3A10%7CWT.mc_id%3Abdcyt__yichewang; dc_search10=baidu%7C%2525E6%252598%252593%2525E8%2525BD%2525A6%2525E7%2525BD%252591; CIGDCID=fc6c1e2cfecb71ae12d281042eb1137c; bitauto_ipregion=36.110.73.210%3a%e5%8c%97%e4%ba%ac%e5%b8%82%e5%8c%97%e4%ba%ac%e5%b8%82%3b201%2c%e5%8c%97%e4%ba%ac%2cbeijing")
                .enableAjax().end().jsonFetcher()
                .configRootType(JsonFetcher.JSONType.ARRAY)
                .configSelector("url", "url", JsonFetcher.JSONType.OBJECT).end()
                .build();

        PageExtract page2 = PageExtract.builder().httpSender()
                .addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
                .addHeader("Accept-Encoding", "gzip, deflate, sdch")
                .addHeader("Accept-Language", "zh-CN,zh;q=0.8")
                .addHeader("Cache-Control", "max-age=0")
                .addHeader("Cookie", "XCWEBLOG_testcookie=yes; locatecity=110100; UserGuid=2aaea016-ac76-4b92-bf3b-e3c8028582c7; BitAutoLogId=479ad4dfe4777813f93c1a6b45cfd3ea; _dadtpqcuq=2b03c51d4a1cae5c4ae5bf5be98cc6672c429089; CarStateForBitAuto=ae591959-2d06-1c0d-1661-d9e830ee1521; BitAutoUserCode=dbb97454-5f9c-d7b6-035d-884ad6645be8; doubleADCookie=%2C2931%3A0%2C2922%3A0%2C2926%3A1%2C3181%3A0%2C3467%3A0%2C3087%3A0%2C4456%3A0%2C4455%3A0%2C4454%3A0%2C3443%3A0%2C4430%3A0%2C4438%3A1%2C4428%3A0%2C4608%3A0%2C3595%3A0; csids=1879_2045_1661_4616; XCWEBLOG_testcookie=yes; bitauto_ipregion=36.110.73.210%3a%e5%8c%97%e4%ba%ac%e5%b8%82%e5%8c%97%e4%ba%ac%e5%b8%82%3b201%2c%e5%8c%97%e4%ba%ac%2cbeijing; BBC_Active=2017-04-25+14%3a52%3a19; Hm_lvt_a792d274c2df065fd0379a10e992e160=1493026465,1493026521,1493032278,1493103183; Hm_lpvt_a792d274c2df065fd0379a10e992e160=1493103183; _dc3c=1; dmt10=1%7C0%7C0%7Cnews.bitauto.com%2Ftest%2F20161228%2F2106761885.html%7Cwww.bitauto.com%2Fpingce%2F; dmts10=1; dm10=9%7C1493103185%7C2%7Cwww.baidu.com%7C%2Fs%3Fie%3Dutf-8%26f%3D3%26rsv_bp%3D1%26rsv_idx%3D2%26tn%3Dbaiduhome_pg%26wd%3D%25E6%2598%2593%25E8%25BD%25A6%25E7%25BD%2591%26rsv_spt%3D1%26oq%3D%2525E6%252594%2525AF%2525E6%25258C%252581ajax%2525E8%2525AF%2525B7%2525E6%2525B1%252582%2525E7%25259A%252584%2525E7%252588%2525AC%2525E8%252599%2525AB%2525E6%2525A1%252586%2525E6%25259E%2525B6%26rsv_pq%3Db221b861000034c8%26rsv_t%3D744ccxgECWdFFy6kHuMK4zMJYu%252BPZ7AvLefF9IsiELbsSX%252FN4KA1S%252BYiwrVy3rbxHZ6t%26rqlang%3Dcn%26rsv_enter%3D1%26inputT%3D402882%26rsv_sug3%3D46%26bs%3D%25E6%2594%25AF%25E6%258C%2581ajax%25E8%25AF%25B7%25E6%25B1%2582%25E7%259A%2584%25E7%2588%25AC%25E8%2599%25AB%25E6%25A1%2586%25E6%259E%25B6%7C%7C%7C1492682857%7C1493030244%7C1493030244%7C1493103185%7Cfc6c1e2cfecb71ae12d281042eb1137c%7C0%7C%7C; dcad10=utm_campaign%3A201704_4008%7Cutm_source%3Abitauto%7Cutm_medium%3A10%7CWT.mc_id%3Abdcyt__yichewang; dc_search10=baidu%7C%2525E6%252598%252593%2525E8%2525BD%2525A6%2525E7%2525BD%252591; CIGDCID=fc6c1e2cfecb71ae12d281042eb1137c")
                .end().htmlFetcher()
                .configSingleValueColumn("title", "article>h1[class='tit-h1']", null)
                .configSingleValueColumn("content", "article>div[class='article-content']", null).end()
                .enbalePageUrlOutput("url")
                .addConstColumnOutput("source", "易车网")
                .build();

        Storage storage = new HBaseStorage("xuming.car_article");

        String rootUrl = "http://news.bitauto.com/views3/news/tagnewsjsonlist?cid=1088&callback=getTagNewsData&pageIndex=%d";
        String rootRefer = "http://news.bitauto.com/";
        for (int i = 1; i <= 172; i++) {
            String realUrl = String.format(rootUrl, i);

            new PipeLine().pipe(realUrl, rootRefer, storage, page1, page2);
        }

        PipeLine.close(storage, page1, page2);
    }
}
