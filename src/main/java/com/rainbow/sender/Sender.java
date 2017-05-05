package com.rainbow.sender;

import java.io.IOException;

/**
 * Created by xuming on 2017/4/25.
 */
public interface Sender {
    String sendPostReq(String url, String jsonData);
    String sendGetReq(String url) throws IOException;
    void close();
}
