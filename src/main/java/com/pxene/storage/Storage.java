package com.pxene.storage;

import com.pxene.fetcher.FetchResult;

import java.io.Closeable;
import java.io.IOException;

/**
 * Created by xuming on 2017/4/26.
 */
public interface Storage extends Closeable {
    void save(FetchResult result) throws IOException;
}
