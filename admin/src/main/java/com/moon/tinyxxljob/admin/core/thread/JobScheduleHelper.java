package com.moon.tinyxxljob.admin.core.thread;

import com.moon.tinyxxljob.admin.core.conf.XxlJobAdminConfig;
import com.moon.tinyxxljob.admin.core.consts.SqlConst;
import com.moon.tinyxxljob.admin.core.cron.CronExpression;
import com.moon.tinyxxljob.admin.core.model.XxlJobInfo;
import com.moon.tinyxxljob.admin.core.scheduler.MisfireStrategyEnum;
import com.moon.tinyxxljob.admin.core.scheduler.ScheduleTypeEnum;
import com.moon.tinyxxljob.admin.core.trigger.TriggerTypeEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * 任务调度器，不断扫描数据库，获取任务执行时间，计算任务的下次执行时间
 *
 * @author Chanmoey
 * Create at 2024/2/26
 */
public class JobScheduleHelper {

    private Logger logger = LoggerFactory.getLogger(JobScheduleHelper.class);

    // 单例
    private static JobScheduleHelper instance = new JobScheduleHelper();

    // 单例方法
    public static JobScheduleHelper getInstance() {
        return instance;
    }

    // 5秒，每次扫描当前时间+5秒内要执行的任务
    public static final long PRE_READ_MS = 5000;

    /**
     * 实际工作的线程，这个线程将不断地轮询数据库，查询待执行的任务
     * 主要工作：
     * 1. 查询任务
     * 2. 触发任务调度（另一个线程去做）
     * 3. 计算任务下次执行时间
     */
    private Thread scheduleThread;

    /**
     * 时间轮线程
     * 向触发器线程提交触发任务
     * 任务从Map中获得，Map中的任务通过scheduleThread来添加
     */
    private Thread ringThread;

    private volatile boolean scheduleThreadToStop = false;
    private volatile boolean ringThreadToStop = false;

    /**
     * 时间轮容器
     * key：秒刻度
     * value：该刻度内，需要执行的任务
     */
    private volatile static Map<Integer, List<Integer>> ringData = new ConcurrentHashMap<>();

    /**
     * 启动调度器
     */
    public void start() {
        this.scheduleThread = new Thread(() -> {
            try {
                // 主要是整秒后再启动
                TimeUnit.MILLISECONDS.sleep(PRE_READ_MS - System.currentTimeMillis() % 1000);
            } catch (InterruptedException e) {
                if (!scheduleThreadToStop) {
                    logger.error(e.getMessage(), e);
                }
            }

            logger.info(">>>>>>>>>>> init xxl-job admin scheduler success.");
            // 6000, 数据库取出的任务数限制为6000
            int preReadCount = (XxlJobAdminConfig.getAdminConfig().getTriggerPoolFastMax() + XxlJobAdminConfig.getAdminConfig().getTriggerPoolSlowMax()) * 20;

            // 调度器开始工作
            while (!scheduleThreadToStop) {
                // 得到当前调度任务的开始时间（用于判断扫描数据库耗费了多少时间）
                long start = System.currentTimeMillis();

                // 数据库连接
                Connection conn = null;
                // 是否自动提交事务
                Boolean connAutoCommit = null;

                PreparedStatement preparedStatement = null;

                // 是否从数据库中扫描到了任务
                boolean preReadSuc = true;

                try {
                    // 获取连接
                    conn = XxlJobAdminConfig.getAdminConfig().getDataSource().getConnection();
                    // 获取事务的默认提交模式
                    connAutoCommit = conn.getAutoCommit();
                    // 获取数据库锁
                    preparedStatement = conn.prepareStatement(SqlConst.LOCK);
                    preparedStatement.execute();

                    // 获取当前时间（用于得到需要调度的任务）
                    long nowTime = System.currentTimeMillis();

                    // 将当前时间及其5秒后需要执行的任务都查询出来，且限制任务数量为6000
                    List<XxlJobInfo> scheduleList = XxlJobAdminConfig.getAdminConfig().getXxlJobInfoDao().scheduleJobQuery(nowTime + PRE_READ_MS, preReadCount);

                    if (scheduleList != null && !scheduleList.isEmpty()) {
                        // 处理每一个任务
                        for (XxlJobInfo jobInfo : scheduleList) {
                            // 可能由于某些原因，导致了当前任务的执行时间已经过了
                            if (nowTime > jobInfo.getTriggerNextTime() + PRE_READ_MS) {
                                logger.warn(">>>>>>>>>>> xxl-job, schedule misfire, jobId = " + jobInfo.getId());
                                // 获取错过执行后的补救策略
                                MisfireStrategyEnum misfireStrategyEnum = MisfireStrategyEnum.match(jobInfo.getMisfireStrategy(), MisfireStrategyEnum.DO_NOTHING);

                                // 是否立刻执行一次
                                if (misfireStrategyEnum.fireOneNow()) {
                                    //在这里立刻执行一次任务
                                    JobTriggerPoolHelper.trigger(jobInfo.getId(), TriggerTypeEnum.MISFIRE, -1, null, null, null);
                                    logger.debug(">>>>>>>>>>> xxl-job, schedule push trigger : jobId = " + jobInfo.getId());
                                }
                                // 根据任务的执行时间去计算任务的下一次计算的时间
                                refreshNextValidTime(jobInfo, new Date());
                            } else if (nowTime > jobInfo.getTriggerNextTime()) {
                                // 当前时间已经超过了任务的执行时间了，但是任务还是在5秒的调度周期内
                                // 因此，直接执行
                                JobTriggerPoolHelper.trigger(jobInfo.getId(), TriggerTypeEnum.CRON, -1, null, null, null);
                                logger.debug(">>>>>>>>>>> xxl-job, schedule push trigger : jobId = " + jobInfo.getId());
                                // 计算任务的下次执行时间
                                refreshNextValidTime(jobInfo, new Date());
                                // 当前任务已经启动，并且下一次执行时间，还在这个调度周期内，则将其放在时间轮中，继续执行
                                if (jobInfo.getTriggerStatus() == 1 && nowTime + PRE_READ_MS > jobInfo.getTriggerNextTime()) {
                                    // 计算当前任务在时间轮中的刻度
                                    int ringSecond = (int) ((jobInfo.getTriggerNextTime() / 1000) % 60);
                                    // 放到时间轮中
                                    pushTimeRing(ringSecond, jobInfo.getId());
                                    // 重新计算要执行的时间
                                    refreshNextValidTime(jobInfo, new Date(jobInfo.getTriggerNextTime()));
                                }
                            } else {
                                // 到了这里，都是在周期内，且没有过期的任务
                                // 直接放到时间轮中
                                int ringSecond = (int) ((jobInfo.getTriggerNextTime() / 1000) % 60);
                                //放进时间轮中
                                pushTimeRing(ringSecond, jobInfo.getId());
                                //刷新定时任务下一次的执行时间
                                refreshNextValidTime(jobInfo, new Date(jobInfo.getTriggerNextTime()));
                            }
                        }
                        // 更新任务的信息 FIXME 使用批处理提高性能
                        for (XxlJobInfo jobInfo : scheduleList) {
                            XxlJobAdminConfig.getAdminConfig().getXxlJobInfoDao().scheduleUpdate(jobInfo);
                        }
                    } else {
                        // 走到这里，说明这扫描周期内，没有找到任何任务
                        preReadSuc = false;
                    }
                } catch (Exception e) {
                    if (!scheduleThreadToStop) {
                        logger.error(">>>>>>>>>>> xxl-job, JobScheduleHelper#scheduleThread error:{}", e);
                    }
                } finally {
                    if (conn != null) {
                        try {
                            // 提交事务
                            conn.commit();
                        } catch (SQLException e) {
                            if (!scheduleThreadToStop) {
                                logger.error(e.getMessage(), e);
                            }
                        }
                        try {
                            // 还原事务的默认提交模式
                            conn.setAutoCommit(connAutoCommit);
                        } catch (SQLException e) {
                            if (!scheduleThreadToStop) {
                                logger.error(e.getMessage(), e);
                            }
                        }
                        try {
                            // 关闭连接
                            conn.close();
                        } catch (SQLException e) {
                            if (!scheduleThreadToStop) {
                                logger.error(e.getMessage(), e);
                            }
                        }
                    }
                    if (null != preparedStatement) {
                        try {
                            preparedStatement.close();
                        } catch (SQLException e) {
                            if (!scheduleThreadToStop) {
                                logger.error(e.getMessage(), e);
                            }
                        }
                    }
                }

                // 再次计算当前时间，然后减去扫库开始的时间
                long cost = System.currentTimeMillis() - start;
                if (cost < 1000) {
                    try {
                        // 我们每隔一秒扫描一次数据库，当这次扫描库的总耗时 < 1秒时，就休息够1秒，等待下一秒再开始
                        // 如果，这次5秒周期内没有任何任务，则把这5秒都睡过去
                        TimeUnit.MILLISECONDS.sleep((preReadSuc ? 1000 : PRE_READ_MS) - System.currentTimeMillis() % 1000);
                    } catch (InterruptedException e) {
                        if (!scheduleThreadToStop) {
                            logger.error(e.getMessage(), e);
                        }
                    }
                }
            }
            logger.info(">>>>>>>>>>> xxl-job, JobScheduleHelper#scheduleThread stop");
        });

        // 设置守护线程，并启动
        scheduleThread.setDaemon(true);
        scheduleThread.setName("xxl-job, admin JobScheduleHelper#scheduleThread");
        scheduleThread.start();

        // 时间环的工作线程
        ringThread = new Thread(() -> {
            while (!ringThreadToStop) {
                try {
                    // 整秒再开始调度
                    TimeUnit.MILLISECONDS.sleep(1000 - System.currentTimeMillis() % 1000);
                } catch (InterruptedException e) {
                    if (!ringThreadToStop) {
                        logger.error(e.getMessage(), e);
                    }
                }
                try {
                    List<Integer> ringItemData = new ArrayList<>();
                    // 获取当前时间的秒数
                    int nowSecond = Calendar.getInstance().get(Calendar.SECOND);
                    // 把当前刻度和前一个刻度都取出来（为了兜底）
                    for (int i = 0; i <= 1; i++) {
                        List<Integer> temData = ringData.remove((nowSecond + 60 - i) % 60);
                        if (temData != null) {
                            ringItemData.addAll(temData);
                        }
                    }
                    logger.debug(">>>>>>>>>>> xxl-job, time-ring beat : " + nowSecond + " = " + Arrays.asList(ringItemData));

                    if (!ringItemData.isEmpty()) {
                        for (int jobId : ringItemData) {
                            JobTriggerPoolHelper.trigger(jobId, TriggerTypeEnum.CRON, -1, null, null, null);
                        }
                        // 最后清空集合
                        ringItemData.clear();
                    }
                } catch (Exception e) {
                    if (!ringThreadToStop) {
                        logger.error(">>>>>>>>>>> xxl-job, JobScheduleHelper#ringThread error:{}", e.getMessage());
                    }
                }
            }
            logger.info(">>>>>>>>>>> xxl-job, JobScheduleHelper#ringThread stop");
        });

        //到这里可以总结一下了，总的来说，xxl-job之所以把任务调度搞得这么复杂，判断了多种情况，引入时间轮
        //就是考虑到某些任务耗时比较严重，结束时间超过了后续任务的执行时间，所以要经常判断前面有没有未执行的任务
        ringThread.setDaemon(true);
        ringThread.setName("xxl-job, admin JobScheduleHelper#ringThread");
        ringThread.start();
    }

    private void refreshNextValidTime(XxlJobInfo jobInfo, Date fromTime) throws Exception {
        Date nextValidTime = generateNextValidTime(jobInfo, fromTime);
        if (nextValidTime != null) {
            jobInfo.setTriggerLastTime(jobInfo.getTriggerNextTime());
            jobInfo.setTriggerNextTime(nextValidTime.getTime());
        } else {
            jobInfo.setTriggerStatus(0);
            jobInfo.setTriggerLastTime(0);
            jobInfo.setTriggerNextTime(0);
            logger.warn(">>>>>>>>>>> xxl-job, refreshNextValidTime fail for job: jobId={}, scheduleType={}, scheduleConf={}",
                    jobInfo.getId(), jobInfo.getScheduleType(), jobInfo.getScheduleConf());
        }
    }

    private void pushTimeRing(int ringSecond, int jobId) {
        List<Integer> ringItemData = ringData.computeIfAbsent(ringSecond, k -> new ArrayList<Integer>());
        ringItemData.add(jobId);
        logger.debug(">>>>>>>>>>> xxl-job, schedule push time-ring : {} = {}", ringSecond, Arrays.asList(ringItemData));
    }

    public void toStop() {
        scheduleThreadToStop = true;
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
        if (scheduleThread.getState() != Thread.State.TERMINATED) {
            scheduleThread.interrupt();
            try {
                scheduleThread.join();
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
        }
        boolean hasRingData = false;
        if (!ringData.isEmpty()) {
            for (int second : ringData.keySet()) {
                List<Integer> tmpData = ringData.get(second);
                if (tmpData != null && !tmpData.isEmpty()) {
                    hasRingData = true;
                    break;
                }
            }
        }
        if (hasRingData) {
            try {
                TimeUnit.SECONDS.sleep(8);
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
        }
        ringThreadToStop = true;
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
        if (ringThread.getState() != Thread.State.TERMINATED) {
            // interrupt and wait
            ringThread.interrupt();
            try {
                ringThread.join();
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
        }
        logger.info(">>>>>>>>>>> xxl-job, JobScheduleHelper stop");
    }

    public static Date generateNextValidTime(XxlJobInfo jobInfo, Date fromTime) throws Exception {
        ScheduleTypeEnum scheduleTypeEnum = ScheduleTypeEnum.match(jobInfo.getScheduleType(), null);
        if (ScheduleTypeEnum.CRON == scheduleTypeEnum) {
            Date nextValidTime = new CronExpression(jobInfo.getScheduleConf()).getNextValidTimeAfter(fromTime);
            return nextValidTime;
        } else if (ScheduleTypeEnum.FIX_RATE == scheduleTypeEnum) {
            return new Date(fromTime.getTime() + Integer.valueOf(jobInfo.getScheduleConf()) * 1000);
        }
        return null;
    }
}
