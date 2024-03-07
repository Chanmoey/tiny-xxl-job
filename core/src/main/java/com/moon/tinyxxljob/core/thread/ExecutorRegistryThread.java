package com.moon.tinyxxljob.core.thread;

import com.moon.tinyxxljob.core.biz.AdminBiz;
import com.moon.tinyxxljob.core.biz.model.RegistryParam;
import com.moon.tinyxxljob.core.biz.model.ReturnT;
import com.moon.tinyxxljob.core.enums.RegistryConfig;
import com.moon.tinyxxljob.core.executor.XxlJobExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * @author Chanmoey
 * Create at 2024-03-08
 */
public class ExecutorRegistryThread {

    private static Logger logger = LoggerFactory.getLogger(ExecutorRegistryThread.class);
    //创建该类的对象
    private static ExecutorRegistryThread instance = new ExecutorRegistryThread();

    //通过该方法将该类对象暴露出去
    public static ExecutorRegistryThread getInstance() {
        return instance;
    }

    //将执行器注册到调度中心的线程，也就是真正干活的线程
    private Thread registryThread;
    //线程是否停止工作的标记
    private volatile boolean toStop = false;

    public void start(final String appname, final String address) {
        if (appname == null || appname.trim().isEmpty()) {
            logger.warn(">>>>>>>>>>> xxl-job, executor registry config fail, appname is null.");
            return;
        }

        // 访问调度中心的客户端不存在
        if (XxlJobExecutor.getAdminBizList() == null) {
            logger.warn(">>>>>>>>>>> xxl-job, executor registry config fail, adminAddresses is null.");
            return;
        }

        registryThread = new Thread(() -> {
            while (!toStop) {
                try {
                    // address 是执行器的地址
                    RegistryParam registryParam = new RegistryParam(RegistryConfig.RegistType.EXECUTOR.name(), appname, address);

                    // 遍历所有的调度中心
                    for (AdminBiz adminBiz : XxlJobExecutor.getAdminBizList()) {
                        try {
                            ReturnT<String> registryResult = adminBiz.registry(registryParam);

                            // 注册成果了
                            if (registryResult != null && ReturnT.SUCCESS_CODE == registryResult.getCode()) {
                                registryResult = ReturnT.SUCCESS;
                                logger.debug(">>>>>>>>>>> xxl-job registry success, registryParam:{}, registryResult:{}", new Object[]{registryParam, registryResult});

                                // 主要一个注册中心注册成功，则在数据库中就写好了执行器和任务的信息，可以退出了
                                break;
                            } else {
                                logger.info(">>>>>>>>>>> xxl-job registry fail, registryParam:{}, registryResult:{}", new Object[]{registryParam, registryResult});
                            }
                        } catch (Exception e) {
                            logger.info(">>>>>>>>>>> xxl-job registry error, registryParam:{}", registryParam, e);
                        }
                    }

                } catch (Exception e) {
                    if (!toStop) {
                        logger.error(e.getMessage(), e);
                    }
                }

                try {
                    if (!toStop) {
                        // 睡眠30秒，30秒一次心跳
                        TimeUnit.SECONDS.sleep(RegistryConfig.BEAT_TIMEOUT);
                    }
                }catch (InterruptedException e) {
                    if (!toStop) {
                        logger.warn(">>>>>>>>>>> xxl-job, executor registry thread interrupted, error msg:{}", e.getMessage());
                    }
                }
            }

            // 到了这里，说明本执行器要下线了
            try {
                RegistryParam registryParam = new RegistryParam(RegistryConfig.RegistType.EXECUTOR.name(), appname, address);
                for (AdminBiz adminBiz: XxlJobExecutor.getAdminBizList()) {
                    try {
                        //在这里发送删除执行器的信息
                        ReturnT<String> registryResult = adminBiz.registryRemove(registryParam);
                        if (registryResult!=null && ReturnT.SUCCESS_CODE == registryResult.getCode()) {
                            registryResult = ReturnT.SUCCESS;
                            logger.info(">>>>>>>>>>> xxl-job registry-remove success, registryParam:{}, registryResult:{}", new Object[]{registryParam, registryResult});
                            break;
                        } else {
                            logger.info(">>>>>>>>>>> xxl-job registry-remove fail, registryParam:{}, registryResult:{}", new Object[]{registryParam, registryResult});
                        }
                    } catch (Exception e) {
                        if (!toStop) {
                            logger.info(">>>>>>>>>>> xxl-job registry-remove error, registryParam:{}", registryParam, e);
                        }
                    }
                }
            } catch (Exception e) {
                if (!toStop) {
                    logger.error(e.getMessage(), e);
                }
            }
            logger.info(">>>>>>>>>>> xxl-job, executor registry thread destroy.");
        });

        //在这里启动线程
        registryThread.setDaemon(true);
        registryThread.setName("xxl-job, executor ExecutorRegistryThread");
        registryThread.start();
    }

    public void toStop() {
        //改变线程是否停止的标记
        toStop = true;
        if (registryThread != null) {
            //中断注册线程
            registryThread.interrupt();
            try {
                //这里就是线程的最基础的知识，在在哪个线程中调用了注册线程的join方法
                //哪个线程就会暂时阻塞住，等待注册线程执行完了才会继续向下执行
                registryThread.join();
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
        }
    }
}
