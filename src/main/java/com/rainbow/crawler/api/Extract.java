package com.rainbow.crawler.api;

import com.rainbow.crawler.fetcher.FetchResult;

import java.io.Closeable;
import java.io.IOException;

/**
 * Created by xuming on 2017/4/26.
 */
public interface Extract extends Closeable {
    FetchResult extract(String url, String refer) throws IOException;
}
