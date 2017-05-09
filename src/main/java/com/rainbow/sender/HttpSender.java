package com.rainbow.sender;

import com.rainbow.api.PageExtract;
import com.rainbow.common.Prediction;
import com.rainbow.common.QueryWordUtil;
import org.apache.commons.httpclient.util.URIUtil;
import org.apache.http.Header;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.BasicHttpClientConnectionManager;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.util.EntityUtils;
import sun.net.URLCanonicalizer;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.UnsupportedCharsetException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by xuming on 2017/4/25.
 */
public class HttpSender implements Sender {
    private Map<String, String> headers = new HashMap<>();
    private CloseableHttpClient client = null;
    private boolean enbaleAjax = false;

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

        if (enbaleAjax) {
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

            if (!QueryWordUtil.isFrequentlyUsed(str)) {
                str = new String(bytes, "gbk");
            }

            if (!QueryWordUtil.isFrequentlyUsed(str)) {
                str = "";
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
            sender.enbaleAjax = true;
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
