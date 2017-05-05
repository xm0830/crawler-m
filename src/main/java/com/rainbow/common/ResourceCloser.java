package com.rainbow.common;

import java.io.Closeable;
import java.io.IOException;

/**
 * Created by xuming on 2017/4/28.
 */
public class ResourceCloser {
    public static void close(Closeable ... resources) {
        if (resources != null) {
            for (Closeable resource : resources) {
                try {
                    if (resource != null) {
                        resource.close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
