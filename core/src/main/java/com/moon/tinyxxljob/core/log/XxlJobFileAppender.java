package com.moon.tinyxxljob.core.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author Chanmoey
 * Create at 2024-03-16
 */
public class XxlJobFileAppender {

    private static Logger logger = LoggerFactory.getLogger(XxlJobFileAppender.class);

    /**
     * 默认日志存储路径
     */
    private static String logBasePath = "/data/applogs/xxl-job/jobhandler";

    /**
     * 保存用户在线编辑的代码
     */
    private static String glueSrcPath = logBasePath.concat("/gluesource");

    public static void initLogPath(String logPath) {
        if (logPath != null && !logPath.trim().isEmpty()) {
            logBasePath = logPath;
        }

        File logPathDir = new File(logBasePath);
        if (!logPathDir.exists()) {
            logPathDir.mkdirs();
        }
        logBasePath = logPathDir.getPath();
        File glueBaseDir = new File(logPathDir, "gluesource");
        if (!glueBaseDir.exists()) {
            glueBaseDir.mkdirs();
        }
        glueSrcPath = glueBaseDir.getPath();
    }

    public static String getLogPath() {
        return logBasePath;
    }

    public static String getGlueSrcPath() {
        return glueSrcPath;
    }

    public static String makeLogFileName(Date triggerDate, long logId) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        File logFilePath = new File(getLogPath(), sdf.format(triggerDate));
        if (!logFilePath.exists()) {
            logFilePath.mkdir();
        }
        return logFilePath.getPath()
                .concat(File.separator)
                .concat(String.valueOf(logId))
                .concat(".log");
    }

    public static void appendLog(String logFileName, String appendLog) {

    }
