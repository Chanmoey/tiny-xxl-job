package com.moon.tinyxxljob.core.biz.util;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * @author Chanmoey
 * Create at 2024-02-29
 */
public class ThrowableUtil {

    public static String toString(Throwable e) {
        StringWriter stringWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stringWriter));
        return stringWriter.toString();
    }
}
