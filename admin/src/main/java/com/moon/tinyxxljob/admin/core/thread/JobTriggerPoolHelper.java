package com.moon.tinyxxljob.admin.core.thread;

import com.moon.tinyxxljob.admin.core.conf.XxlJobAdminConfig;
import com.moon.tinyxxljob.admin.core.trigger.TriggerTypeEnum;
import com.moon.tinyxxljob.admin.core.trigger.XxlJobTrigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 将要被执行的任务包装成触发器
 * 通过快慢两个线程池，分别触发不同类型的任务，提高性能
 *
 * @author Chanmoey
 * Create at 2024/2/27
 */
public class JobTriggerPoolHelper {

    private static Logger logger = LoggerFactory.getLogger(JobTriggerPoolHelper.class);

    // 快线程池
    private ThreadPoolExecutor fastTriggerPool = null;

    // 慢线程池
    private ThreadPoolExecutor slowTriggerPool = null;

    /**
     * 创建快慢两个线程池
     */
    public void start() {
        // 快线程池，最大线程数200
        fastTriggerPool = new ThreadPoolExecutor(
                10,
                XxlJobAdminConfig.getAdminConfig().getTriggerPoolFastMax(),
                60L,
                TimeUnit.SECONDS,
                new LinkedBlockingDeque<>(1000),
                r -> new Thread(r, "xxl-job, admin JobTriggerPoolHelper-fastTriggerPool-" + r.hashCode())
        );

        //慢线程池，最大线程数为100
        slowTriggerPool = new ThreadPoolExecutor(
                10,
                XxlJobAdminConfig.getAdminConfig().getTriggerPoolSlowMax(),
                60L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(2000),
                r -> new Thread(r, "xxl-job, admin JobTriggerPoolHelper-slowTriggerPool-" + r.hashCode()));
    }

    public void stop() {
        fastTriggerPool.shutdownNow();
        slowTriggerPool.shutdownNow();
        logger.info(">>>>>>>>> xxl-job trigger thread pool shutdown success.");
    }

    /**
     * 获取系统当前的时间（当前分钟数）
     */
    private volatile long minTim = System.currentTimeMillis() / 60000;

    /**
     * 如果任务处理时间超过500ms，则任务该任务是慢任务
     * 如果慢任务出现，则将它加入到这个缓存中
     * key表示任务的id，value表示一分钟内，任务执行了的次数
     */
    private volatile ConcurrentMap<Integer, AtomicInteger> jobTimeoutCountMap = new ConcurrentHashMap<>();


    /**
     *
     * @param jobId 任务id
     * @param triggerType 触发类型
     * @param failRetryCount 失败重试次数
     * @param executorShadingParam 执行器分片参数
     * @param executorParam 执行器方法参数
     * @param addressList 执行器地址列表
     */
    public void addTrigger(final int jobId, final TriggerTypeEnum triggerType,
                           final int failRetryCount, final String executorShadingParam,
                           final String executorParam, final String addressList) {
        // 默认使用快线程池
        ThreadPoolExecutor triggerPool = fastTriggerPool;

        AtomicInteger jobTimeoutCount = jobTimeoutCountMap.get(jobId);
        // 当慢任务执行操作了10次，则用慢线程池进行处理
        if (jobTimeoutCount != null && jobTimeoutCount.get() > 10) {
            triggerPool = slowTriggerPool;
        }
        // 将触发定时任务包装成一个任务
        triggerPool.execute(() -> {
            // 再次获取当前时间
            long start = System.currentTimeMillis();
            try {
                // 触发定时任务，这里是同步调用了
                XxlJobTrigger.trigger(jobId, triggerType, failRetryCount, executorShadingParam, executorParam, addressList);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            } finally {
                // 再次获取当前的分钟数
                long minTimNow = System.currentTimeMillis() / 60000;
                // 因为jobTimeoutCountMap是记录一分钟内的，所以这时候要清空Map
                if (minTim != minTimNow) {
                    minTim = minTimNow;
                    jobTimeoutCountMap.clear();
                }
                // 计算任务耗时
                long cost = System.currentTimeMillis() - start;
                if (cost > 500) {
                    // 任务耗时超过500s，则认为是慢任务，记录一分钟内的调度次数
                    AtomicInteger timeoutCount = jobTimeoutCountMap.putIfAbsent(jobId, new AtomicInteger(1));
                    if (timeoutCount != null) {
                        timeoutCount.incrementAndGet();
                    }
                }
            }
        });
    }

    private static JobTriggerPoolHelper helper = new JobTriggerPoolHelper();

    public static void toStart() {
        helper.start();
    }

    public static void toStop() {
        helper.stop();
    }

    public static void trigger(int jobId, TriggerTypeEnum triggerType, int failRetryCount, String executorShardingParam, String executorParam, String addressList) {
        helper.addTrigger(jobId, triggerType, failRetryCount, executorShardingParam, executorParam, addressList);
    }
}
