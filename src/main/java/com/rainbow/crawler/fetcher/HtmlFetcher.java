package com.rainbow.crawler.fetcher;

import com.rainbow.crawler.common.Prediction;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by xuming on 2017/4/25.
 */
public class HtmlFetcher implements Fetcher {

    private Map<String, String> singleValueSelector = new HashMap<>();
    private Map<String, String> singleValueAttr = new HashMap<>();

    private Map<String, String> multiValueSelector = new HashMap<>();
    private Map<String, String> multiValueAttr = new HashMap<>();

    private Map<String, Processor> processors = new HashMap<>();

    private HtmlFetcher() {
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public FetchResult fetch(String html, String url) {
        Prediction.predictNotNullAndEmpty(html);
        Document doc = Jsoup.parse(html);

        FetchResult singleResult = new FetchResult();
        for (Map.Entry<String, String> entry : singleValueSelector.entrySet()) {
            Elements select = doc.select(entry.getValue());

            if (select.size() != 1) {
                throw new RuntimeException("column has " + select.size() + " values for single-value column: " + entry.getKey());
            }

            if (!select.isEmpty()) {
                String attrName = singleValueAttr.get(entry.getKey());
                Processor processor = processors.get(entry.getKey());
                if (attrName != null) {
                    if (processor != null) {
                        singleResult.insertColumn(0, entry.getKey(), processor.process(select.get(0).attr(attrName), url).trim());
                    } else {
                        singleResult.insertColumn(0, entry.getKey(), select.get(0).attr(attrName).trim());
                    }
                } else {
                    if (processor != null) {
                        singleResult.insertColumn(0, entry.getKey(), processor.process(select.get(0).text(), url).trim());
                    } else {
                        singleResult.insertColumn(0, entry.getKey(), select.get(0).text().trim());
                    }
                }
            }
        }

        int rowNum = 0;
        FetchResult multiResult = new FetchResult();
        for (Map.Entry<String, String> entry : multiValueSelector.entrySet()) {
            Elements select = doc.select(entry.getValue());

            if (rowNum == 0) {
                rowNum = select.size();
            }

            if (select.size() == 0) {
                throw new RuntimeException("multi-value columns has no value for key: " + entry.getKey());
            }

            if (select.size() != rowNum) {
                throw new RuntimeException("multi-value columns have several row num!");
            }

            if (!select.isEmpty()) {
                int rowIndex = 0;
                for (int i = 0; i < select.size(); i++) {
                    Element element = select.get(i);

                    String attrName = multiValueAttr.get(entry.getKey());
                    Processor processor = processors.get(entry.getKey());
                    if (attrName != null) {
                        if (processor != null) {
                            multiResult.insertColumn(rowIndex, entry.getKey(), processor.process(element.attr(attrName), url).trim());
                        } else {
                            multiResult.insertColumn(rowIndex, entry.getKey(), element.attr(attrName).trim());
                        }
                    } else {
                        if (processor != null) {
                            multiResult.insertColumn(rowIndex, entry.getKey(), processor.process(element.text(), url).trim());
                        } else {
                            multiResult.insertColumn(rowIndex, entry.getKey(), element.text().trim());
                        }
                    }

                    rowIndex++;
                }
            }
        }

        return FetchResult.merge(singleResult, multiResult);
    }

    public static class Builder extends FetcherBuilder {
        private HtmlFetcher fetcher = new HtmlFetcher();

        private Builder() {}

        public Builder configSingleValueColumn(String name, String cssSelector, String attr) {
            Prediction.predictNotNullAndEmpty(name);
            Prediction.predictNotNullAndEmpty(cssSelector);

            fetcher.singleValueSelector.put(name, cssSelector);
            if (attr != null && !"".equals(attr)) {
                fetcher.singleValueAttr.put(name, attr);
            }
            return this;
        }

        public Builder configMultiValueColumn(String name, String cssSelector, String attr) {
            Prediction.predictNotNullAndEmpty(name);
            Prediction.predictNotNullAndEmpty(cssSelector);

            fetcher.multiValueSelector.put(name, cssSelector);
            if (attr != null && !"".equals(attr)) {
                fetcher.multiValueAttr.put(name, attr);
            }
            return this;
        }

        public Builder configValueProcessor(String name, Processor processor) {
            Prediction.predictNotNullAndEmpty(name);
            Prediction.predictNotNull(processor);

            fetcher.processors.put(name, processor);
            return this;
        }

        @Override
        public HtmlFetcher build() {
            return fetcher;
        }
    }
}
