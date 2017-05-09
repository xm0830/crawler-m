package com.rainbow.sender;

import com.rainbow.api.PageExtract;
import org.apache.commons.httpclient.util.URIUtil;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.jsoup.Jsoup;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by xuming on 2017/4/25.
 */
public class HttpSender implements Sender {
    public static final Logger logger = LoggerFactory.getLogger(HttpSender.class);

    private Map<String, String> headers = new HashMap<>();
    private CloseableHttpClient client = null;
    private boolean enableAjax = false;

    private HttpSender() {
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public String sendPostReq(String url, String jsonData) {
        return null;
    }

    @Override
    public String sendGetReq(String url) throws IOException {
        HttpGet get = new HttpGet(URIUtil.encodePathQuery(url));

        RequestConfig config = RequestConfig.custom()
                .setConnectTimeout(180000).setConnectionRequestTimeout(180000)
                .setSocketTimeout(180000).build();

        get.setConfig(config);

        for (Map.Entry<String, String> entry : headers.entrySet()) {
            get.addHeader(entry.getKey(), entry.getValue());
        }

        if (!headers.containsKey("User-Agent")) {
            get.addHeader("User-Agent", "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36");
        }

        if (enableAjax) {
            get.addHeader("X-Requested-With", "XMLHttpRequest");
        }

        CloseableHttpResponse resp = client.execute(get);
        int status = resp.getStatusLine().getStatusCode();

        if (status == HttpStatus.SC_OK) {
            ContentType contentType = ContentType.get(resp.getEntity());
            if (contentType != null) {
                if (contentType.getCharset() == null) {
                    contentType = contentType.withCharset("utf-8");
                }
            } else {
                contentType = ContentType.DEFAULT_TEXT.withCharset("utf-8");
            }

            byte[] bytes = EntityUtils.toByteArray(resp.getEntity());
            String str = new String(bytes, contentType.getCharset());

            Elements select = Jsoup.parse(str).select("meta[http-equiv=\"Content-Type\"]");
            if (select.size() > 0) {
                String content = select.get(select.size() - 1).attr("content");
                String[] split = content.split(";");
                if (split.length == 2) {
                    String[] split1 = split[1].split("=");
                    if (split1.length == 2) {
                        String charset = split1[1];
                        str = new String(bytes, charset);

                        logger.info("it has a sense for charset type: " + charset);
                    }
                }
            }

            resp.close();
            return str;
        }

//        if (status >= HttpStatus.SC_MULTIPLE_CHOICES && status <= HttpStatus.SC_TEMPORARY_REDIRECT) {
//            get.abort();
//        }

        get.abort();

        resp.close();
        return "";
    }

    @Override
    public void close() {
        try {
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static class Builder {
        private HttpSender sender = new HttpSender();
        private PageExtract.Builder context = null;

        private Builder() {}

        public Builder addHeader(String key, String value) {
            sender.headers.put(key, value);
            return this;
        }

        public Builder enableAjax() {
            sender.enableAjax = true;
            return this;
        }

        public void setContext(PageExtract.Builder context) {
            this.context = context;
        }

        public PageExtract.Builder end() {
            return context;
        }

        public HttpSender build() {
            sender.client = HttpClients.createDefault();
            return sender;
        }
    }
}
