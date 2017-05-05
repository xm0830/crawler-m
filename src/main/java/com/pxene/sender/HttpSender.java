package com.pxene.sender;

import com.pxene.api.PageExtract;
import com.pxene.common.Prediction;
import org.apache.commons.httpclient.util.URIUtil;
import org.apache.http.Header;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.BasicHttpClientConnectionManager;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.util.EntityUtils;
import sun.net.URLCanonicalizer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by xuming on 2017/4/25.
 */
public class HttpSender implements Sender {
    private Map<String, String> headers = new HashMap<>();
    private CloseableHttpClient client = null;
    private boolean enbaleAjax = false;
    private String charset = "utf-8";

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
        HttpGet get = new HttpGet(URIUtil.encodeQuery(url));

        RequestConfig config = RequestConfig.custom()
                .setConnectTimeout(30000).setConnectionRequestTimeout(30000)
                .setSocketTimeout(30000).build();

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
            String str = EntityUtils.toString(resp.getEntity(), charset);
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

        public Builder setCharset(String charset) {
            Prediction.predictNotNullAndEmpty(charset);

            sender.charset = charset;
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
