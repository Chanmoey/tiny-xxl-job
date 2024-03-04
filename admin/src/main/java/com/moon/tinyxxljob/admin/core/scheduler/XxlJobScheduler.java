package com.moon.tinyxxljob.admin.core.scheduler;

import com.moon.tinyxxljob.admin.core.conf.XxlJobAdminConfig;
import com.moon.tinyxxljob.admin.core.thread.JobRegistryHelper;
import com.moon.tinyxxljob.admin.core.thread.JobScheduleHelper;
import com.moon.tinyxxljob.admin.core.thread.JobTriggerPoolHelper;
import com.moon.tinyxxljob.core.biz.ExecutorBiz;
import com.moon.tinyxxljob.core.biz.client.ExecutorBizClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 可以认为这个类是调度中心的启动类，负责启动调度中心的各个模块
 *
 * @author Chanmoey
 * Create at 2024/2/26
 */
public class XxlJobScheduler {

    private static final Logger logger = LoggerFactory.getLogger(XxlJobScheduler.class);

    public void init() throws Exception {
        // 初始化夫发起线程池，创建快慢两个线程池
        JobTriggerPoolHelper.toStart();

        // 初始化注册中心
        JobRegistryHelper.getInstance().start();

        // 初始化任务调度线程，xxl-job的核心
        JobScheduleHelper.getInstance().start();

    }

    public void destroy() {
        JobScheduleHelper.getInstance().toStop();
        JobRegistryHelper.getInstance().toStop();
        JobTriggerPoolHelper.toStop();
    }

    private static ConcurrentMap<String, ExecutorBiz> executorBizRepository = new ConcurrentHashMap<>();

    /**
     * 获取远程调用的客户端
     */
    public static ExecutorBiz getExecutorBiz(String address) throws Exception {



        if (address == null || (address = address.trim()).isEmpty()) {
            return null;
        }

        ExecutorBiz executorBiz = executorBizRepository.get(address);
        if (executorBiz != null) {
            return executorBiz;
        }

        // 新建客户端
        executorBiz = new ExecutorBizClient(address, XxlJobAdminConfig.getAdminConfig().getAccessToken());
        // 缓存起来
        executorBizRepository.put(address, executorBiz);

        return executorBiz;
    }

}
