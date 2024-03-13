package com.moon.tinyxxljob.core.biz.impl;

import com.moon.tinyxxljob.core.biz.ExecutorBiz;
import com.moon.tinyxxljob.core.biz.model.IdleBeatParam;
import com.moon.tinyxxljob.core.biz.model.ReturnT;
import com.moon.tinyxxljob.core.biz.model.TriggerParam;
import com.moon.tinyxxljob.core.executor.XxlJobExecutor;
import com.moon.tinyxxljob.core.glue.GlueTypeEnum;
import com.moon.tinyxxljob.core.handler.IJobHandler;
import com.moon.tinyxxljob.core.thread.JobThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Chanmoey
 * Create at 2024-03-06
 */
public class ExecutorBizImpl implements ExecutorBiz {

    private static Logger logger = LoggerFactory.getLogger(ExecutorBizImpl.class);


    @Override
    public ReturnT<String> beat() {
        return null;
    }

    @Override
    public ReturnT<String> idleBeat(IdleBeatParam idleBeatParam) {
        return null;
    }

    @Override
    public ReturnT<String> run(TriggerParam triggerParam) {

        // 通过任务ID获取负责执行任务的线程
        JobThread jobThread = XxlJobExecutor.loadJobThread(triggerParam.getJobId());

        IJobHandler jobHandler = jobThread != null ? jobThread.getHandler() : null;

        String removeOldReason = null;

        GlueTypeEnum glueTypeEnum = GlueTypeEnum.match(triggerParam.getGlueType());

        if (GlueTypeEnum.BEAN == glueTypeEnum) {
            IJobHandler newJobHandler = XxlJobExecutor.loadJobHandler(triggerParam.getExecutorHandler());

            // jobHandler不为空，说明该任务已经执行过，并且分配了线程
            // 但newJobHandler不相等，说明是新的任务进来
            if (jobThread != null && jobHandler != newJobHandler) {
                // kill掉旧线程
                removeOldReason = "change jobHandler or glue type, and terminate the old job thread.";
                jobThread = null;
                jobHandler = null;

            }
            if (jobHandler == null) {
                jobHandler = newJobHandler;
                if (jobHandler == null) {
                    // 没有对应的jobHandler
                    return new ReturnT<String>(ReturnT.FAIL_CODE, "job handler [" + triggerParam.getExecutorHandler() + "] not found.");
                }
            }
        } else {
            return new ReturnT<String>(ReturnT.FAIL_CODE, "glueType[" + triggerParam.getGlueType() + "] is not valid.");
        }
        if (jobThread == null) {
            jobThread = XxlJobExecutor.registerJobThread(triggerParam.getJobId(), jobHandler, removeOldReason);
        }
        // 同步等待线程执行完成定时任务，并返回结果
        return jobThread.pushTriggerQueue(triggerParam);
    }
}
