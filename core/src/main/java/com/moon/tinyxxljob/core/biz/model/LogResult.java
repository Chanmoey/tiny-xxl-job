package com.moon.tinyxxljob.core.biz.model;

import java.io.Serializable;

/**
 * @author Chanmoey
 * Create at 2024-03-18
 */
public class LogResult implements Serializable {

    private int fromLineNum;
    private int toLineNum;
    private String logContent;
    private boolean isEnd;

    private static final long serialVersionUID = 42L;

    public LogResult() {

    }

    public LogResult(int fromLineNum, int toLineNum, String logContent, boolean isEnd) {
        this.fromLineNum = fromLineNum;
        this.toLineNum = toLineNum;
        this.logContent = logContent;
        this.isEnd = isEnd;
    }

    public int getFromLineNum() {
        return fromLineNum;
    }

    public void setFromLineNum(int fromLineNum) {
        this.fromLineNum = fromLineNum;
    }

    public int getToLineNum() {
        return toLineNum;
    }

    public void setToLineNum(int toLineNum) {
        this.toLineNum = toLineNum;
    }

    public String getLogContent() {
        return logContent;
    }

    public void setLogContent(String logContent) {
        this.logContent = logContent;
    }

    public boolean isEnd() {
        return isEnd;
    }

    public void setEnd(boolean end) {
        isEnd = end;
    }
}