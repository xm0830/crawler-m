package com.rainbow.api;

import com.rainbow.common.Prediction;
import com.rainbow.fetcher.*;
import com.rainbow.sender.HttpSender;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by xuming on 2017/4/25.
 */
public class PageExtract implements Extract {
    private HttpSender.Builder senderBuilder = HttpSender.builder();
    private FetcherBuilder fetcherBuilder = null;

    private HttpSender sender = null;
    private Fetcher fetcher = null;

    private long reqIntervalTime = 1000;

    private String pageUrlName = null;

    private Map<String, String> constColumns = new HashMap<>();

    private PageExtract() {

    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public FetchResult extract(String url, String refer) throws IOException {

        senderBuilder.addHeader("Referer", refer);

        if (sender == null) {
            sender = senderBuilder.build();
        }

        if (fetcher == null) {
            fetcher = fetcherBuilder.build();
        }

        String str = sender.sendGetReq(url);
        FetchResult result = fetcher.fetch(str, url);

        if (result.size() > 0) {
            if (pageUrlName != null && !"".equals(pageUrlName)) {
                result.addColumn(pageUrlName, url);
            }

            for (Map.Entry<String, String> entry : constColumns.entrySet()) {
                result.addColumn(entry.getKey(), entry.getValue());
            }
        }

        try {
            Thread.sleep(reqIntervalTime);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return result;
    }

    @Override
    public void close() {
        if (sender != null) {
            sender.close();
            sender = null;
        }
    }

    public static class Builder {

        private PageExtract extract = new PageExtract();

        private Builder() {
        }

        public PageExtract build() {
            return extract;
        }

        public Builder setReqIntervalTime(int millis) {
            extract.reqIntervalTime = millis;

            return this;
        }

        public Builder enbalePageUrlOutput(String name) {
            Prediction.predictNotNullAndEmpty(name);

            extract.pageUrlName = name;

            return this;
        }

        public Builder addConstColumnOutput(String name, String value) {
            Prediction.predictNotNullAndEmpty(name);
            Prediction.predictNotNullAndEmpty(value);

            extract.constColumns.put(name, value);

            return this;
        }

        public HttpSender.Builder httpSender() {
            extract.senderBuilder.setContext(this);
            return extract.senderBuilder;
        }

        public HtmlFetcher.Builder htmlFetcher() {
            extract.fetcherBuilder = HtmlFetcher.builder();
            extract.fetcherBuilder.setContext(this);
            return (HtmlFetcher.Builder) extract.fetcherBuilder;
        }

        public JsonFetcher.Builder jsonFetcher() {
            extract.fetcherBuilder = JsonFetcher.builder();
            extract.fetcherBuilder.setContext(this);
            return (JsonFetcher.Builder) extract.fetcherBuilder;
        }
    }
}
