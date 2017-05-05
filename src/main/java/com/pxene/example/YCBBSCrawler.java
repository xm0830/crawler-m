package com.pxene.example;

import com.pxene.api.PageExtract;
import com.pxene.api.PipeLine;
import com.pxene.fetcher.FetchResult;
import com.pxene.fetcher.JsonFetcher;
import com.pxene.fetcher.Processor;
import com.pxene.scheduler.TimeRangeRule;
import com.pxene.scheduler.TimeRangeSheduler;
import com.pxene.storage.HBaseStorage;
import com.pxene.storage.Storage;

import java.io.IOException;
import java.util.List;

/**
 * Created by xuming on 2017/5/3.
 */
public class YCBBSCrawler {
    public static void main(String[] args) throws IOException {
        PageExtract brandPage = PageExtract.builder().httpSender()
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

        PageExtract carPage = PageExtract.builder().httpSender()
                .addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
                .addHeader("Accept-Encoding", "gzip, deflate, sdch")
                .addHeader("Accept-Language", "zh-CN,zh;q=0.8")
                .addHeader("Cookie", "XCWEBLOG_testcookie=yes; locatecity=110100; XCWEBLOG_testcookie=yes; _dc3c=1; _dcisnw=1; dmt10=2%7C0%7C0%7Ccar.bitauto.com%2F%2Ftree_chexing%2Fmb_9%2F%7C; dmts10=1; dm10=1%7C1493272012%7C0%7C%7C%7C%7C%7C1493271475%7C1493271475%7C0%7C1493271475%7Cfc6c1e2cfecb71ae12d281042eb1137c%7C0%7C%7C; dcad10=; dc_search10=; CIGDCID=fc6c1e2cfecb71ae12d281042eb1137c; bitauto_ipregion=36.110.73.210%3a%e5%8c%97%e4%ba%ac%e5%b8%82%e5%8c%97%e4%ba%ac%e5%b8%82%3b201%2c%e5%8c%97%e4%ba%ac%2cbeijing; Hm_lvt_7b86db06beda666182190f07e1af98e3=1493031766,1493265453,1493272012; Hm_lpvt_7b86db06beda666182190f07e1af98e3=1493272012")
                .addHeader("Upgrade-Insecure-Requests", "1").end().htmlFetcher()
                .configMultiValueColumn("url", "#divCsLevel_0 > div ul.p-list > li:nth-child(1) > a", "href")
                .configValueProcessor("url", new Processor() {
                    @Override
                    public String process(String value, String url) {
                        return "http://car.bitauto.com" + value + "pingce/";
                    }
                }).end()
                .build();

        PageExtract itemPage = PageExtract.builder().httpSender()
                .addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
                .addHeader("Accept-Encoding", "gzip, deflate, sdch")
                .addHeader("Accept-Language", "zh-CN,zh;q=0.8")
                .addHeader("Cookie", "XCWEBLOG_testcookie=yes; locatecity=110100; XCWEBLOG_testcookie=yes; _dc3c=1; _dcisnw=1; dmt10=2%7C0%7C0%7Ccar.bitauto.com%2F%2Ftree_chexing%2Fmb_9%2F%7C; dmts10=1; dm10=1%7C1493272012%7C0%7C%7C%7C%7C%7C1493271475%7C1493271475%7C0%7C1493271475%7Cfc6c1e2cfecb71ae12d281042eb1137c%7C0%7C%7C; dcad10=; dc_search10=; CIGDCID=fc6c1e2cfecb71ae12d281042eb1137c; bitauto_ipregion=36.110.73.210%3a%e5%8c%97%e4%ba%ac%e5%b8%82%e5%8c%97%e4%ba%ac%e5%b8%82%3b201%2c%e5%8c%97%e4%ba%ac%2cbeijing; Hm_lvt_7b86db06beda666182190f07e1af98e3=1493031766,1493265453,1493272012; Hm_lpvt_7b86db06beda666182190f07e1af98e3=1493272012")
                .addHeader("Upgrade-Insecure-Requests", "1").end().htmlFetcher().end().htmlFetcher()
                .configSingleValueColumn("url", "#CN_ShowLastLink", "href").end()
                .build();


        PageExtract listPage = PageExtract.builder().httpSender()
                .addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
                .addHeader("Accept-Encoding", "gzip, deflate, sdch")
                .addHeader("Accept-Language", "zh-CN,zh;q=0.8")
                .addHeader("Cookie", "XCWEBLOG_testcookie=yes; locatecity=110100; CarStateForBitAuto=d7352bf5-36ba-eecd-ef9b-cdbbbcbe0543; BitAutoUserCode=2bf9c54d-c19f-a7c2-80ca-110f6a441cb9; UserGuid=ec368689-e53c-4242-907d-c932d5deb115; BitAutoLogId=7f8401f52f700bb5fbf0ecd6b621e7d0; doubleADCookie=%2C2931%3A1%2C2922%3A0%2C2926%3A1%2C3181%3A0%2C3467%3A0%2C3087%3A0%2C4456%3A0%2C4455%3A0%2C4454%3A0%2C3443%3A0%2C4430%3A0%2C4438%3A1%2C4428%3A0%2C4608%3A0; _dc3c=1; ASP.NET_SessionId=40s1d41l5dz02xoctfuit534; bdshare_firstime=1493807183208; csids=2593_3547_3736_2075_2582_1987; bitauto_ipregion=36.110.73.210%3a%e5%8c%97%e4%ba%ac%e5%b8%82%3b201%2c%e5%8c%97%e4%ba%ac%2cbeijing; Hm_lvt_8f7b0f02f06be2d533aa2273ea6f7028=1492685717,1492685794,1493107940,1493807968; Hm_lpvt_8f7b0f02f06be2d533aa2273ea6f7028=1493807968; dmt10=13%7C1%7C0%7Cbaa.bitauto.com%2Faodia4%2Fthread-6772586.html%7Cbaa.bitauto.com%2Faodia4%2F; dmts10=1; dm10=8%7C1493807968%7C2%7Cwww.baidu.com%7C%2Fs%3Fie%3Dutf-8%26f%3D8%26rsv_bp%3D1%26rsv_idx%3D1%26tn%3Dmswin_oem_dg%26wd%3D%25E6%2598%2593%25E8%25BD%25A6%26oq%3D%2525E6%2525B1%2525BD%2525E8%2525BD%2525A6%2525E4%2525B9%25258B%2525E5%2525AE%2525B6%26rsv_pq%3D9ebb859e00004855%26rsv_t%3Dcef4f%252B7%252FT8y2vDBlwvqiZ8L%252FhJkgmg8qp1HzyrN5SyYH6osVROiXbxKlFKeN7i3450Yf%26rqlang%3Dcn%26rsv_enter%3D1%26inputT%3D2648%26rsv_sug3%3D39%26rsv_sug1%3D36%26rsv_sug7%3D100%26rsv_sug2%3D0%26rsv_sug4%3D3481%7C%7C%7C1493277576%7C1493807162%7C1493366237%7C1493807162%7Cfc6c1e2cfecb71ae12d281042eb1137c%7C0%7C%7C; dcad10=WT.mc_id%3Abdcyt__yiche; dc_search10=baidu%7C%2525E6%252598%252593%2525E8%2525BD%2525A6; CIGDCID=fc6c1e2cfecb71ae12d281042eb1137c")
                .addHeader("Upgrade-Insecure-Requests", "1").end().htmlFetcher()
                .configMultiValueColumn("url", "#divTopicList > div > ul:nth-child(1) > li.bt > a", "href").end()
                .build();

        PageExtract resultPage = PageExtract.builder().httpSender()
                .addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
                .addHeader("Accept-Encoding", "gzip, deflate, sdch")
                .addHeader("Accept-Language", "zh-CN,zh;q=0.8")
                .addHeader("Cache-Control", "max-age=0")
                .addHeader("Cookie", "XCWEBLOG_testcookie=yes; locatecity=110100; CarStateForBitAuto=d7352bf5-36ba-eecd-ef9b-cdbbbcbe0543; BitAutoUserCode=2bf9c54d-c19f-a7c2-80ca-110f6a441cb9; UserGuid=ec368689-e53c-4242-907d-c932d5deb115; BitAutoLogId=7f8401f52f700bb5fbf0ecd6b621e7d0; doubleADCookie=%2C2931%3A1%2C2922%3A0%2C2926%3A1%2C3181%3A0%2C3467%3A0%2C3087%3A0%2C4456%3A0%2C4455%3A0%2C4454%3A0%2C3443%3A0%2C4430%3A0%2C4438%3A1%2C4428%3A0%2C4608%3A0; _dc3c=1; ASP.NET_SessionId=40s1d41l5dz02xoctfuit534; bdshare_firstime=1493807183208; csids=2593_3547_3736_2075_2582_1987; Hm_lvt_8f7b0f02f06be2d533aa2273ea6f7028=1492685717,1492685794,1493107940,1493807968; Hm_lpvt_8f7b0f02f06be2d533aa2273ea6f7028=1493807968; bitauto_ipregion=36.110.73.210%3a%e5%8c%97%e4%ba%ac%e5%b8%82%3b201%2c%e5%8c%97%e4%ba%ac%2cbeijing; dmt10=17%7C1%7C0%7Ccar.bitauto.com%2F404error.aspx%7C; dmts10=1; dm10=8%7C1493808828%7C2%7Cwww.baidu.com%7C%2Fs%3Fie%3Dutf-8%26f%3D8%26rsv_bp%3D1%26rsv_idx%3D1%26tn%3Dmswin_oem_dg%26wd%3D%25E6%2598%2593%25E8%25BD%25A6%26oq%3D%2525E6%2525B1%2525BD%2525E8%2525BD%2525A6%2525E4%2525B9%25258B%2525E5%2525AE%2525B6%26rsv_pq%3D9ebb859e00004855%26rsv_t%3Dcef4f%252B7%252FT8y2vDBlwvqiZ8L%252FhJkgmg8qp1HzyrN5SyYH6osVROiXbxKlFKeN7i3450Yf%26rqlang%3Dcn%26rsv_enter%3D1%26inputT%3D2648%26rsv_sug3%3D39%26rsv_sug1%3D36%26rsv_sug7%3D100%26rsv_sug2%3D0%26rsv_sug4%3D3481%7C%7C%7C1493277576%7C1493807162%7C1493366237%7C1493807162%7Cfc6c1e2cfecb71ae12d281042eb1137c%7C0%7C%7C; dcad10=WT.mc_id%3Abdcyt__yiche; dc_search10=baidu%7C%2525E6%252598%252593%2525E8%2525BD%2525A6; CIGDCID=fc6c1e2cfecb71ae12d281042eb1137c")
                .addHeader("Upgrade-Insecure-Requests", "1").end().htmlFetcher()
                .configSingleValueColumn("title", "div.postcontbox > div.postcont_list.post_fist > div > div.postright > div.post_text > div.post_fist_title > div.title_box > h1", null)
                .configSingleValueColumn("content", "div.postcontbox > div.postcont_list.post_fist > div > div.postright > div.post_text.post_text_sl > div.post_width", null)
                .configSingleValueColumn("bbs", "#TitleForumLink", null).end()
                .enbalePageUrlOutput("url")
                .addConstColumnOutput("source", "易车网")
                .build();


        PipeLine pipeLine = new PipeLine(new TimeRangeSheduler(), new TimeRangeRule("0700", "2300"));

        String rootUrl = "http://api.car.bitauto.com/CarInfo/getlefttreejson.ashx?tagtype=chexing&pagetype=masterbrand&objid=0";
        String rootRefer = "http://car.bitauto.com/";
        FetchResult result = pipeLine.pipe(rootUrl, rootRefer, brandPage, carPage, itemPage);
        PipeLine.close(brandPage, carPage, itemPage);

        Storage storage = new HBaseStorage("xuming.car_bbs");

        List<String> urls = result.select("url");
        for (String url : urls) {
            String tplUrl = url + "%s";
            String tplRefer = url + "%s";
            for (int i = 1; ; i++) {
                String realUrl = null;
                String realRefer = null;
                if (i == 1) {
                    realUrl = String.format(tplUrl, "");
                    realRefer = String.format(tplRefer, "");
                } else {
                    realUrl = String.format(tplUrl, String.format("index-all-all-%s-0.html", i));
                    realRefer = String.format(tplRefer, String.format("index-all-all-%s-0.html", i - 1));
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
