package com.rainbow.crawler.api;

import com.rainbow.crawler.common.Prediction;
import com.rainbow.crawler.common.ResourceCloser;
import com.rainbow.crawler.fetcher.FetchResult;
import com.rainbow.crawler.scheduler.ScheduleRule;
import com.rainbow.crawler.scheduler.Scheduler;
import com.rainbow.crawler.storage.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by xuming on 2017/4/26.
 */
public class PipeLine {

    private static final Logger logger = LoggerFactory.getLogger(PipeLine.class);

    private Scheduler scheduler = null;
    private ScheduleRule[] rules = null;

    public PipeLine() { }

    public PipeLine(Scheduler scheduler, ScheduleRule... rules) {
        Prediction.predictNotNull(scheduler);
        Prediction.predictNotNull(rules);

        this.scheduler = scheduler;
        this.rules = rules;
    }

    public FetchResult pipe(String url, String refer, Extract... extracts) {
        Prediction.predictNotNull(url);
        Prediction.predictNotNull(refer);
        Prediction.predictNotNull(extracts);

        List<String> urls = new ArrayList<>();
        urls.add(url);

        return goWithNoStorage(urls, refer, extracts, 0);
    }

    public long pipe(String url, String refer, Storage storage, Extract... extracts) {
        Prediction.predictNotNull(url);
        Prediction.predictNotNull(refer);
        Prediction.predictNotNull(storage);
        Prediction.predictNotNull(extracts);

        List<String> urls = new ArrayList<>();
        urls.add(url);

        return goWithStorage(urls, refer, extracts, 0, storage);
    }

    public static void close(Closeable... resources) {
        ResourceCloser.close(resources);
    }

    private long goWithStorage(List<String> urls, String refer, Extract[] extracts, int index, Storage storage) {
        block(storage, extracts);

        long count = 0;
        for (String url : urls) {
            logger.info(String.format("begin to fetch from page: %s, refer url: %s", url, refer));
            try {
                if (index < extracts.length) {
                    FetchResult result = extracts[index].extract(url, refer);
                    logger.info("fetch result: " + System.lineSeparator() + result.toString());

                    if (index == extracts.length - 1) {
                        try {
                            count += result.size();
                            if (storage != null) {
                                storage.save(result);
                            }
                        } catch (IOException e) {
                            logger.error("error when save data: ", e);
                            throw new RuntimeException(e);
                        }
                    } else {
                        count += goWithStorage(result.select("url"), url, extracts, index + 1, storage);
                    }
                }
            } catch (Exception e) {
                logger.error("error when fetch from page: " + url, e);
            }
        }

        return count;
    }

    private FetchResult goWithNoStorage(List<String> urls, String refer, Extract[] extracts, int index) {
        block(null, extracts);

        FetchResult rs = new FetchResult();
        for (int i = 0; i< urls.size(); i++) {
            String url = urls.get(i);
            logger.info(String.format("begin to fetch from page: %s, refer url: %s", url, refer));
            try {
                if (index < extracts.length) {
                    FetchResult result = extracts[index].extract(url, refer);
                    logger.info("fetch result: " + System.lineSeparator() + result.toString());

                    if (index == extracts.length - 1) {
                        rs.append(result);
                    } else {
                        rs.append(goWithNoStorage(result.select("url"), url, extracts, index + 1));
                    }
                }
            } catch (Exception e) {
                logger.error("error when fetch from page: " + url, e);
            }
        }

        return rs;
    }

    private void block(Closeable resources1, Closeable... resources2) {
        if (scheduler != null) {
            boolean hasLogged = false;
            while (!scheduler.schedule(rules)) {
                ResourceCloser.close(resources1);
                ResourceCloser.close(resources2);

                try {
                    if (!hasLogged)  {
                        logger.info("it is not matches scheduled time, wait...");
                        hasLogged = true;
                    }

                    Thread.sleep(1000 * 10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
