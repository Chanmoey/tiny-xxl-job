package com.moon.tinyxxljob.core.handler;

/**
 * @author Chanmoey
 * Create at 2024-03-05
 */
public interface IJobHandler {

    /**
     * 执行定时任务
     * @throws Exception
     */
    void execute() throws Exception;

    void init() throws Exception;

    void destroy() throws Exception;
}
