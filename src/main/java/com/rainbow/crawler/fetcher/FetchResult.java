package com.rainbow.crawler.fetcher;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by xuming on 2017/4/25.
 */
public class FetchResult {
    private List<Map<String, String>> list = new ArrayList<>();

    public void insertColumn(int rowIndex, String name, String value) {
        if (list.size() - 1 >= rowIndex) {
            list.get(rowIndex).put(name, value);
        } else {
            Map<String, String> row = new HashMap<>();
            row.put(name, value);

            list.add(row);
        }
    }

    public void addColumn(String name, String value) {
        if (!list.isEmpty()) {
            for (Map<String, String> row : list) {
                row.put(name, value);
            }
        }
    }

    public void addFetchResult(FetchResult result) {
        int rowIndex = list.size();
        for (Map<String, String> map : result.list) {
            for (Map.Entry<String, String> entry : map.entrySet()) {
                insertColumn(rowIndex, entry.getKey(), entry.getValue());
            }

            rowIndex++;
        }
    }

    public List<String> select(String name) {
        List<String> result = new ArrayList<>();
        for (Map<String, String> map : list) {
            result.add(map.get(name));
        }

        return result;
    }

    public List<Map<String, String>> getData() {
        return list;
    }

    public int size() {
        return list.size();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < list.size(); i++) {
            Map<String, String> map = list.get(i);
            for (Map.Entry<String, String> entry : map.entrySet()) {
                String value = entry.getValue();
                if (value.length() > 50) value = value.substring(0, 50) + "...";
                sb.append("\t").append(entry.getKey()).append("=>").append(value).append(",");
            }

            if (map.size() > 0) {
                sb.deleteCharAt(sb.length() - 1);
            }

            if (i < list.size() - 1) {
                sb.append(System.lineSeparator());
            }
        }

        return sb.toString();
    }

    static FetchResult merge(FetchResult singleValueResult, FetchResult multiValueResult) {
        if (multiValueResult.size() > 0) {
            if (singleValueResult.size() > 0) {
                for (Map.Entry<String, String> entry : singleValueResult.getData().get(0).entrySet()) {
                    for (int i = 0; i < multiValueResult.size(); i++) {
                        Map<String, String> map = multiValueResult.getData().get(i);
                        map.put(entry.getKey(), entry.getValue());
                    }
                }
            }

            return multiValueResult;
        } else {
            if (singleValueResult.size() > 0) {
                return singleValueResult;
            }
        }

        return new FetchResult();
    }

}
