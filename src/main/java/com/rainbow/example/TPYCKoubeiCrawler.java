package com.rainbow.example;

import com.rainbow.api.PageExtract;
import com.rainbow.api.PipeLine;
import com.rainbow.fetcher.FetchResult;
import com.rainbow.fetcher.JsonFetcher;
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
public class TPYCKoubeiCrawler {
    public static void main(String[] args) throws IOException {
        PageExtract brandsPage = PageExtract.builder().httpSender()
                .addHeader("Accept", "*/*")
                .addHeader("Accept-Encoding", "gzip, deflate, sdch")
                .addHeader("Accept-Language", "zh-CN,zh;q=0.8")
                .addHeader("Cookie", "locationddPro=%u5317%u4EAC%u5E02; TsYes=1; visitedfid=15001; __PCautoPrice4s_area_id_=2-%u5317%u4EAC-2; captcha=e4cc2e59f6da042feedbe1c15bc8a049b12d671428257835785385080; locationCity=%u5317%u4EAC; PClocation=%u5317%u4EAC; pcLocate=%7B%22proCode%22%3A%22110000%22%2C%22pro%22%3A%22%E5%8C%97%E4%BA%AC%E5%B8%82%E7%9C%81%22%2C%22cityCode%22%3A%22110000%22%2C%22city%22%3A%22%E5%8C%97%E4%BA%AC%E5%B8%82%22%2C%22dataType%22%3A%22user%22%2C%22expires%22%3A1495082300898%7D; pcautoLocate=%7B%22proId%22%3A6%2C%22cityId%22%3A2%2C%22url%22%3A%22http%3A%2F%2Fwww.pcauto.com.cn%2Fqcbj%2Fbj%2F%22%2C%22dataTypeAuto%22%3A%22user%22%7D; pcsuv=1493710518299.a.263107321; pcuvdata=lastAccessTime=1493786413363|visits=17; channel=7598").end().jsonFetcher()
                .configRootType(JsonFetcher.JSONType.OBJECT)
                .configSelector("url", "brands", JsonFetcher.JSONType.ARRAY)
                .configSelector("url", "id", JsonFetcher.JSONType.OBJECT)
                .configProcessor("url", new Processor() {
                    @Override
                    public String process(String value, String url) {
                        return String.format("http://price.pcauto.com.cn/comment/interface/comment_serial_group_json_chooser.jsp?type=1&brand=%s&callback=selectInputF0", value);
                    }
                }).end()
                .build();

        PageExtract carPage = PageExtract.builder().httpSender()
                .addHeader("Accept", "*/*")
                .addHeader("Accept-Encoding", "gzip, deflate, sdch")
                .addHeader("Accept-Language", "zh-CN,zh;q=0.8")
                .addHeader("Cookie", "locationddPro=%u5317%u4EAC%u5E02; TsYes=1; visitedfid=15001; __PCautoPrice4s_area_id_=2-%u5317%u4EAC-2; captcha=e4cc2e59f6da042feedbe1c15bc8a049b12d671428257835785385080; locationCity=%u5317%u4EAC; PClocation=%u5317%u4EAC; pcLocate=%7B%22proCode%22%3A%22110000%22%2C%22pro%22%3A%22%E5%8C%97%E4%BA%AC%E5%B8%82%E7%9C%81%22%2C%22cityCode%22%3A%22110000%22%2C%22city%22%3A%22%E5%8C%97%E4%BA%AC%E5%B8%82%22%2C%22dataType%22%3A%22user%22%2C%22expires%22%3A1495082300898%7D; pcautoLocate=%7B%22proId%22%3A6%2C%22cityId%22%3A2%2C%22url%22%3A%22http%3A%2F%2Fwww.pcauto.com.cn%2Fqcbj%2Fbj%2F%22%2C%22dataTypeAuto%22%3A%22user%22%7D; pcsuv=1493710518299.a.263107321; pcuvdata=lastAccessTime=1493786413363|visits=17; channel=7598").end()
                .jsonFetcher()
                .configRootType(JsonFetcher.JSONType.OBJECT)
                .configSelector("id", "firms", JsonFetcher.JSONType.ARRAY)
                .configSelector("id", "id", JsonFetcher.JSONType.OBJECT)
                .configProcessor("id", new Processor() {
                    @Override
                    public String process(String value, String url) {
                        if (value.startsWith("+"))
                            return "";
                        return value;
                    }
                }).end()
                .build();

        PageExtract resultPage = PageExtract.builder().httpSender()
                .addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
                .addHeader("Accept-Encoding", "gzip, deflate, sdch")
                .addHeader("Accept-Language", "zh-CN,zh;q=0.8")
                .addHeader("Cache-Control", "max-age=0")
                .addHeader("Cookie", "locationddPro=%u5317%u4EAC%u5E02; TsYes=1; visitedfid=15001; __PCautoPrice4s_area_id_=2-%u5317%u4EAC-2; captcha=e4cc2e59f6da042feedbe1c15bc8a049b12d671428257835785385080; locationCity=%u5317%u4EAC; PClocation=%u5317%u4EAC; pcLocate=%7B%22proCode%22%3A%22110000%22%2C%22pro%22%3A%22%E5%8C%97%E4%BA%AC%E5%B8%82%E7%9C%81%22%2C%22cityCode%22%3A%22110000%22%2C%22city%22%3A%22%E5%8C%97%E4%BA%AC%E5%B8%82%22%2C%22dataType%22%3A%22user%22%2C%22expires%22%3A1495082300898%7D; pcautoLocate=%7B%22proId%22%3A6%2C%22cityId%22%3A2%2C%22url%22%3A%22http%3A%2F%2Fwww.pcauto.com.cn%2Fqcbj%2Fbj%2F%22%2C%22dataTypeAuto%22%3A%22user%22%7D; pcsuv=1493710518299.a.263107321; pcuvdata=lastAccessTime=1493786510172|visits=17; channel=7598")
                .addHeader("Upgrade-Insecure-Requests", "1").end().htmlFetcher()
                .configSingleValueColumn("title", "#subNav > div.subNav-t > div.subNav-mark > div.title > h1", null)
                .configMultiValueColumn("content", "#main > div.main_body > div.main_table > table > tbody > tr > td:nth-last-child(1) > div > div.table_text", null).end()
                .enbalePageUrlOutput("url")
                .addConstColumnOutput("source", "太平洋汽车网")
                .build();

        PipeLine pipeLine = new PipeLine(new TimeRangeSheduler(), new TimeRangeRule("0700", "2300"));

        String brandUrl = "http://price.pcauto.com.cn/comment/interface/comment_brand_json_chooser.jsp?callback=selectInputB0";
        String brandRefer = "http://price.pcauto.com.cn/comment";
        FetchResult result = pipeLine.pipe(brandUrl, brandRefer, brandsPage, carPage);
        PipeLine.close(brandsPage, carPage);

        Storage storage = new HBaseStorage("xuming.car_koubei");

        List<String> ids = result.select("id");
        String tplCommentUrl = "http://price.pcauto.com.cn/comment/sg%s/%s";
        for (int i = 0; i < ids.size(); i++) {
            String carId = ids.get(i);

            if (carId.equals("")) continue;

            for (int j = 1; ; j++) {
                String tmpUrl = null;
                String tmpRefer = null;
                if (j == 1) {
                    tmpUrl = String.format(tplCommentUrl, carId, "");
                    tmpRefer = "http://price.pcauto.com.cn/comment";
                } else {
                    tmpUrl = String.format(tplCommentUrl, carId, "p" + j + ".html");
                    tmpRefer = String.format(tplCommentUrl, carId, "p" + (j - 1) + ".html");
                }

                long count = pipeLine.pipe(tmpUrl, tmpRefer, storage, resultPage);
                if (count <= 0) {
                    break;
                }
            }
        }

        PipeLine.close(storage, resultPage);
    }
}
