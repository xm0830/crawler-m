package com.pxene.fetcher;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.pxene.common.Prediction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Created by xuming on 2017/4/25.
 */
public class JsonFetcher implements Fetcher {
    private JSONType rootType = JSONType.OBJECT;
    private Map<String, List<String>> selectors = new HashMap<>();
    private Map<String, List<JSONType>> types = new HashMap<>();
    private Map<String, Processor> processors = new HashMap<>();

    private JsonFetcher() {
    }

    public static JsonFetcher.Builder builder() {
        return new JsonFetcher.Builder();
    }

    @Override
    public FetchResult fetch(String html, String url) {
        int beginIndex = 0;
        int endIndex = html.length();

        for (int j = 0; j < html.length(); j++) {
            if (html.charAt(j) != '{' && html.charAt(j) != '[') {
                beginIndex++;
            } else {
                break;
            }
        }

        for (int j = 0; j < html.length(); j++) {
            if (html.charAt(html.length()-j-1) != '}' && html.charAt(html.length()-j-1) != ']' ) {
                endIndex--;
            } else {
                break;
            }
        }

        String jsonStr = html.substring(beginIndex, endIndex);

        Object obj = null;
        FetchResult result = null;
        if (rootType == JSONType.OBJECT) {
            obj = JSONObject.parse(jsonStr);
            result = toResult(obj, JSONType.OBJECT, url);
        } else if (rootType == JSONType.ARRAY) {
            obj = JSONArray.parse(jsonStr);
            result = toResult(obj, JSONType.ARRAY, url);
        } else {
            throw new RuntimeException("not support type: " + rootType.toString());
        }

        return result;
    }

    private FetchResult toResult(Object obj, JSONType type, String url) {
        FetchResult result = new FetchResult();

        for (String key : selectors.keySet()) {
            List<String> tmpSelectors = selectors.get(key);
            List<JSONType> tmpTypes = types.get(key);

            List<String> values = getValues(obj, type, tmpSelectors, tmpTypes, 0);

            for (int i = 0; i < values.size(); i++) {
                Processor processor = processors.get(key);
                if (processor != null) {
                    values.set(i, processor.process(values.get(i), url));
                }
            }

            int rowIndex = 0;
            for (String value : values) {
                result.insertColumn(rowIndex, key, value);
                rowIndex++;
            }
        }
        return result;
    }

    private static List<String> getValues(Object obj, JSONType type, List<String> selectors, List<JSONType> types, int index) {
        List<String> result = new ArrayList<>();

        if (type == JSONType.ARRAY) {
            JSONArray arr = (JSONArray) obj;
            for (int i = 0; i < arr.size(); i++) {
                JSONObject o = (JSONObject) arr.get(i);

                if (index < selectors.size()) {
                    result.addAll(getValues(o, JSONType.OBJECT, selectors, types, index));
                }
            }
        } else if (type == JSONType.OBJECT) {
            JSONObject obj1 = (JSONObject) obj;

            if (index < selectors.size()) {
                if (index == selectors.size() - 1) {
                    for (Map.Entry<String, Object> entry : obj1.entrySet()) {
                        if (Pattern.compile(selectors.get(index)).matcher(entry.getKey()).matches()) {
                            if (entry.getValue() != null) {
                                result.add(entry.getValue().toString().trim());
                            }
                        }
                    }
//                    String value = obj1.getString(selectors.get(index));
//                    if (value != null) {
//                        result.add(value);
//                    }
                } else {
                    for (Map.Entry<String, Object> entry : obj1.entrySet()) {
                        if (Pattern.compile(selectors.get(index)).matcher(entry.getKey()).matches()) {
                            result.addAll(getValues(entry.getValue(), types.get(index), selectors, types, index + 1));
                        }
                    }
//                    result.addAll(getValues(obj1.get(selectors.get(index)), types.get(index), selectors, types, index + 1));
                }
            }
        }

        return result;
    }

    public static class Builder extends FetcherBuilder {
        private JsonFetcher fetcher = new JsonFetcher();

        private Builder() {}

        public Builder configRootType(JSONType type) {
            fetcher.rootType = type;
            return this;
        }

        public Builder configSelector(String name, String selector, JSONType type) {
            Prediction.predictNotNullAndEmpty(name);
            Prediction.predictNotNullAndEmpty(selector);

            add(fetcher.selectors, name, selector);
            add(fetcher.types, name, type);

            return this;
        }

        public Builder configProcessor(String name, Processor processor) {
            Prediction.predictNotNullAndEmpty(name);
            Prediction.predictNotNull(processor);

            fetcher.processors.put(name, processor);
            return this;
        }


        @Override
        public JsonFetcher build() {
            return fetcher;
        }

        private <T> void add(Map<String, List<T>> map, String name, T value) {
            List<T> ts = map.get(name);
            if (ts != null) {
                ts.add(value);
            } else {
                List<T> l = new ArrayList<>();
                l.add(value);

                map.put(name, l);
            }
        }
    }

    public enum JSONType {
        ARRAY,
        OBJECT
    }

    public static void main(String[] args) {
        String str = "[{'test1': 1, 'test2': [{'aa': 'hah', 'bb': 'hah2'}, {'aa': 'hah3', 'bb': 'hah4'}]}," +
                "{'test1': 3, 'test2': [{'aa': 'hah5', 'bb': 'hah6'}, {'aa': 'hah7', 'bb': 'hah8'}]}," +
                "{'test1': 5, 'test2': [{'aa': 'hah9', 'bb': 'hah10'}, {'aa': 'hah11', 'bb': 'hah12'}]}]";
        Object obj = JSONArray.parse(str);
        List<String> selectors = new ArrayList<>();
        selectors.add("test2");
        selectors.add("bb");
        List<JSONType> types = new ArrayList<>();
        types.add(JSONType.ARRAY);
        types.add(JSONType.OBJECT);
        List<String> values = getValues(obj, JSONType.ARRAY, selectors, types, 0);
        for (String value : values) {
            System.out.println(value);
        }
    }
}
