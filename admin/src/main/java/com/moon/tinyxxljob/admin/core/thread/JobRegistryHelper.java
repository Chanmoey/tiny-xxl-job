package com.moon.tinyxxljob.admin.core.thread;

import com.moon.tinyxxljob.admin.core.conf.XxlJobAdminConfig;
import com.moon.tinyxxljob.admin.core.model.XxlJobGroup;
import com.moon.tinyxxljob.admin.core.model.XxlJobRegistry;
import com.moon.tinyxxljob.core.biz.model.RegistryParam;
import com.moon.tinyxxljob.core.biz.model.ReturnT;
import com.moon.tinyxxljob.core.enums.RegistryConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.support.SmartTransactionObject;
import org.springframework.util.StringUtils;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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

    /**
     * 心跳检测线程
     */
    private Thread registryMonitorThread;

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

        registryMonitorThread = new Thread(() -> {
            while (!toStop) {
                try {
                    List<XxlJobGroup> groupList = XxlJobAdminConfig.getAdminConfig().getXxlJobGroupDao().findByAddressType(0);
                    if (groupList != null && !groupList.isEmpty()) {
                        // 获取所有超时的id
                        List<Integer> ids = XxlJobAdminConfig.getAdminConfig().getXxlJobRegistryDao().findDead(RegistryConfig.DEAD_TIMEOUT, new Date());
                        if (ids != null && !ids.isEmpty()) {
                            // 删除过期的执行器
                            XxlJobAdminConfig.getAdminConfig().getXxlJobRegistryDao().removeDead(ids);
                        }

                        Map<String, List<String>> appAddressMap = new HashMap<>();
                        // 获取所有没过期的执行器
                        List<XxlJobRegistry> list = XxlJobAdminConfig.getAdminConfig().getXxlJobRegistryDao().findAll(RegistryConfig.DEAD_TIMEOUT, new Date());

                        if (list != null) {
                            for (XxlJobRegistry item : list) {
                                // 判断是否为自动注册的
                                if (RegistryConfig.RegistType.EXECUTOR.name().equals(item.getRegistryGroup())) {
                                    String appname = item.getRegistryKey();
                                    // 获取所有同名的执行器，也就是一组
                                    List<String> registryList = appAddressMap.get(appname);

                                    if (registryList == null) {
                                        registryList = new ArrayList<>();
                                    }

                                    // TODO 这里改成Set会不会更好？
                                    if (!registryList.contains(item.getRegistryValue())) {
                                        registryList.add(item.getRegistryValue());
                                    }

                                    appAddressMap.put(appname, registryList);
                                }
                            }
                        }

                        for (XxlJobGroup group : groupList) {
                            List<String> registryList = appAddressMap.get(group.getAppname());
                            String addressListStr = null;
                            if (registryList != null && !registryList.isEmpty()) {
                                // 源码选择排序，我不想排序
                                // Collections.sort(registryList);
                                StringBuilder addressSb = new StringBuilder();
                                for (String address : registryList) {
                                    addressSb.append(address).append(',');
                                }
                                // 删除最后一个,
                                addressSb.deleteCharAt(addressSb.length() - 1);
                                addressListStr = addressSb.toString();
                            }

                            group.setAddressList(addressListStr);
                            group.setUpdateTime(new Date());
                            XxlJobAdminConfig.getAdminConfig().getXxlJobGroupDao().update(group);
                        }
                    }
                } catch (Exception e) {
                    if (!toStop) {
                        logger.error(">>>>>>>>>>> xxl-job, job registry monitor thread error:{}", e);
                    }
                }
            }

            try {
                //线程在这里睡30秒，也就意味着检测周期为30秒
                TimeUnit.SECONDS.sleep(RegistryConfig.BEAT_TIMEOUT);
            } catch (InterruptedException e) {
                if (!toStop) {
                    logger.error(">>>>>>>>>>> xxl-job, job registry monitor thread error:{}", e);
                }
            }
        });
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
