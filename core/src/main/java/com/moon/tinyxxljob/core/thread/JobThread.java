package com.moon.tinyxxljob.core.thread;

import com.moon.tinyxxljob.core.biz.model.ReturnT;
import com.moon.tinyxxljob.core.biz.model.TriggerParam;
import com.moon.tinyxxljob.core.executor.XxlJobExecutor;
import com.moon.tinyxxljob.core.handler.IJobHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * 执行定时任务的线程
 *
 * @author Chanmoey
 * Create at 2024-03-05
 */
public class JobThread extends Thread {

    private static Logger logger = LoggerFactory.getLogger(JobThread.class);

    // 定时任务的id
    private int jobId;

    private IJobHandler handler;

    private LinkedBlockingQueue<TriggerParam> triggerQueue;

    private volatile boolean toStop = false;

    private String stopReason;

    private boolean running = false;

    private int idleTimes = 0;

    public JobThread(int jobId, IJobHandler handler) {
        this.jobId = jobId;
        this.handler = handler;
        //初始化队列
        this.triggerQueue = new LinkedBlockingQueue<TriggerParam>();
        //设置工作线程名称
        this.setName("xxl-job, JobThread-" + jobId + "-" + System.currentTimeMillis());
    }

    public IJobHandler getHandler() {
        return handler;
    }

    public ReturnT<String> pushTriggerQueue(TriggerParam triggerParam) {
        //在这里放进队列中
        triggerQueue.add(triggerParam);
        //返回成功结果
        return ReturnT.SUCCESS;
    }

    public void toStop(String stopReason) {
        //把线程终止标记设为true
        this.toStop = true;
        this.stopReason = stopReason;
    }

    public boolean isRunningOrHasQueue() {
        return running || !triggerQueue.isEmpty();
    }

    @Override
    public void run() {
        try {
            // 执行任务的初始化方法
            handler.init();
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
        }

        while (!toStop) {
            running = false;
            idleTimes++;
            TriggerParam triggerParam = null;

            try {
                triggerParam = triggerQueue.poll(3L, TimeUnit.SECONDS);
                if (triggerParam != null) {
                    running = true;
                    idleTimes = 0;
                    // 执行定时任务
                    handler.execute();
                } else {
                    // 一直没有新任务，直接停掉这个线程
                    if (idleTimes > 30) {
                        if (triggerQueue.isEmpty()) {
                            XxlJobExecutor.removeJobThread(jobId, "executor idle times over limit.");
                        }
                    }
                }
            } catch (Throwable e) {
                if (toStop) {
                    logger.info("<br>----------- JobThread toStop, stopReason:" + stopReason);
                }
            }
        }

        // 线程被停掉了，收尾工作
        try {
            handler.destroy();
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
        }

        logger.info(">>>>>>>>>>> xxl-job JobThread stopped, hashCode:{}", Thread.currentThread());
    }
}
