package com.rainbow.fetcher;

import com.rainbow.api.PageExtract;

/**
 * Created by xuming on 2017/4/26.
 */
public abstract class FetcherBuilder {
    private PageExtract.Builder context = null;

    public void setContext(PageExtract.Builder context) {
        this.context = context;
    }
    public PageExtract.Builder end() {
        return context;
    }

    public abstract Fetcher build();
}
