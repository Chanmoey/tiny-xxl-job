package com.moon.tinyxxljob.admin.core.thread;

import com.moon.tinyxxljob.admin.core.conf.XxlJobAdminConfig;
import com.moon.tinyxxljob.core.biz.model.RegistryParam;
import com.moon.tinyxxljob.core.biz.model.ReturnT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.util.Date;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author Chanmoey
 * Create at 2024-03-04
 */
public class JobRegistryHelper {

    private static Logger logger = LoggerFactory.getLogger(JobRegistryHelper.class);

    private static JobRegistryHelper instance = new JobRegistryHelper();

    public static JobRegistryHelper getInstance() {
        return instance;
    }

    /**
     * 负责注册或移除执行器
     */
    private ThreadPoolExecutor registryOrRemoveThreadPool = null;

    private volatile boolean toStop = false;

    public void start() {
        registryOrRemoveThreadPool = new ThreadPoolExecutor(
                2,
                10,
                30L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(2000),
                r -> new Thread(r, "xxl-job, admin JobRegistryMonitorHelper-registryOrRemoveThreadPool-" + r.hashCode()),
                // 拒绝策略，直接由添加任务的线程执行新的任务
                (r, e) -> {
                    r.run();
                    logger.warn(">>>>>>>>>>> xxl-job, registry or remove too fast, match threadpool rejected handler(run now).");
                }
        );
    }

    public void toStop() {
        toStop = true;
        registryOrRemoveThreadPool.shutdownNow();
    }

    public ReturnT<String> registry(RegistryParam registryParam) {
        if (!StringUtils.hasText(registryParam.getRegistryGroup())
                || !StringUtils.hasText(registryParam.getRegistryKey())
                || !StringUtils.hasText(registryParam.getRegistryValue())) {
            return new ReturnT<>(ReturnT.FAIL_CODE, "Illegal Argument.");
        }

        registryOrRemoveThreadPool.execute(() -> {
            // 尝试更新
            int ret = XxlJobAdminConfig.getAdminConfig().getXxlJobRegistryDao().registryUpdate(registryParam.getRegistryGroup(), registryParam.getRegistryKey(), registryParam.getRegistryValue(), new Date());
            // 更新失败，说明数据库中没有数据
            if (ret < 1) {
                //这里就是数据库中没有相应数据，直接新增即可
                XxlJobAdminConfig.getAdminConfig().getXxlJobRegistryDao().registrySave(registryParam.getRegistryGroup(), registryParam.getRegistryKey(), registryParam.getRegistryValue(), new Date());
                //该方法从名字上看是刷新注册表信息的意思
                freshGroupRegistryInfo(registryParam);
            }
        });

        return ReturnT.SUCCESS;
    }

    public ReturnT<String> registryRemove(RegistryParam registryParam) {
        if (!StringUtils.hasText(registryParam.getRegistryGroup())
                || !StringUtils.hasText(registryParam.getRegistryKey())
                || !StringUtils.hasText(registryParam.getRegistryValue())) {
            return new ReturnT<String>(ReturnT.FAIL_CODE, "Illegal Argument.");
        }

        registryOrRemoveThreadPool.execute(new Runnable() {
            @Override
            public void run() {
                // 在这里直接根据registryParam从数据库中删除对应的执行器地址
                // 这里的返回结果是删除了几条数据的意思
                int ret = XxlJobAdminConfig.getAdminConfig().getXxlJobRegistryDao().registryDelete(registryParam.getRegistryGroup(), registryParam.getRegistryKey(), registryParam.getRegistryValue());
                if (ret > 0) {
                    freshGroupRegistryInfo(registryParam);
                }
            }
        });
        return ReturnT.SUCCESS;
    }

    private void freshGroupRegistryInfo(RegistryParam registryParam){
    }
}
