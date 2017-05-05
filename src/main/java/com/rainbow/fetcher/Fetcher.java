package com.rainbow.fetcher;

/**
 * Created by xuming on 2017/4/25.
 */
public interface Fetcher {
    FetchResult fetch(String html, String url);
}
