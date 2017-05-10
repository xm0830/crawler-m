package com.rainbow.crawler.common;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created by xuming on 2017/4/21.
 */
public class Prediction {

    public static void predictNotNull(Object obj) {
        if (obj == null) {
            throw new RuntimeException("unexpected! instance of " + obj.getClass().getName() + " is null!");
        }
    }

    public static <T> void predictNotEmpty(T obj) {
        try {
            String str = (String) obj;
            if (str.isEmpty()) {
                throw new RuntimeException("unexpected! instance of " + obj.getClass().getName() + " is empty!");
            }
        } catch (ClassCastException e) {
            try {
                Collection collection = (Collection) obj;
                if (collection.isEmpty()) {
                    throw new RuntimeException("unexpected! instance of " + obj.getClass().getName() + " is empty!");
                }
            } catch (ClassCastException e1) {
                throw new RuntimeException(obj.getClass().getName() + " has no isEmpty method!");
            }
        }
    }

    public static <T> void predictNotNullAndEmpty(T obj) {
        predictNotNull(obj);
        predictNotEmpty(obj);
    }

    public static void main(String[] args) {
        List<String> list = new ArrayList<>();
        predictNotEmpty(list);
    }
}
