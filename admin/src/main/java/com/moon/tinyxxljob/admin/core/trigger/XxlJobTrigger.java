package com.moon.tinyxxljob.admin.core.trigger;


import com.moon.tinyxxljob.admin.core.conf.XxlJobAdminConfig;
import com.moon.tinyxxljob.admin.core.model.XxlJobGroup;
import com.moon.tinyxxljob.admin.core.model.XxlJobInfo;
import com.moon.tinyxxljob.admin.core.router.ExecutorRouteStrategyEnum;
import com.moon.tinyxxljob.admin.core.scheduler.XxlJobScheduler;
import com.moon.tinyxxljob.admin.core.util.I18nUtil;
import com.moon.tinyxxljob.core.biz.ExecutorBiz;
import com.moon.tinyxxljob.core.biz.model.ReturnT;
import com.moon.tinyxxljob.core.biz.model.TriggerParam;
import com.moon.tinyxxljob.core.biz.util.ThrowableUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author Chanmoey
 * Create at 2024/2/27
 */
public class XxlJobTrigger {

    private static Logger logger = LoggerFactory.getLogger(XxlJobTrigger.class);

    public static void trigger(int jobId, TriggerTypeEnum triggerType, int failRetryCount,
                               String executorShadingParam, String executorParam, String addressList) {
        XxlJobInfo jobInfo = XxlJobAdminConfig.getAdminConfig().getXxlJobInfoDao().loadById(jobId);
        if (jobInfo == null) {
            logger.warn(">>>>>>>>>>>> trigger fail, jobId invalid，jobId={}", jobId);
            return;
        }
        // 如果用户传了参数，则设置进去
        if (executorParam != null) {
            jobInfo.setExecutorParam(executorParam);
        }
        // 查询当前任务的所有执行器
        XxlJobGroup group = XxlJobAdminConfig.getAdminConfig().getXxlJobGroupDao().load(jobInfo.getJobGroup());

        // 这里的addressList不为空，说明是用户在web页面手动设置的
        if (addressList != null && !addressList.trim().isEmpty()) {
            group.setAddressType(1);
            group.setAddressList(addressList.trim());
        }

        // 去触发定时任务
        processTrigger(group, jobInfo, -1, triggerType, 0, 1);
    }

    private static void processTrigger(XxlJobGroup group, XxlJobInfo jobInfo,
                                       int finalFailRetryCount, TriggerTypeEnum triggerType,
                                       int index, int total) {
        ExecutorRouteStrategyEnum executorRouteStrategyEnum = ExecutorRouteStrategyEnum.match(jobInfo.getExecutorRouteStrategy(), ExecutorRouteStrategyEnum.RANDOM);

        TriggerParam triggerParam = new TriggerParam();
        triggerParam.setJobId(jobInfo.getId());
        triggerParam.setExecutorHandler(jobInfo.getExecutorHandler());
        triggerParam.setExecutorParams(jobInfo.getExecutorParam());
        triggerParam.setGlueType(jobInfo.getGlueType());
        String address = null;
        ReturnT<String> routeAddressResult;
        List<String> registryList = group.getRegistryList();
        // 路由策略，选择一个地址
        if (registryList != null && !registryList.isEmpty()) {
            routeAddressResult = executorRouteStrategyEnum.getRouter().route(triggerParam, registryList);
            if (ReturnT.SUCCESS_CODE == routeAddressResult.getCode()) {
                address = routeAddressResult.getContent();
            } else {
                routeAddressResult = new ReturnT<String>(ReturnT.FAIL_CODE, I18nUtil.getString("jobconf_trigger_address_empty"));
            }
        }
        ReturnT<String> triggerResult;
        if (address != null) {
            // 进行远程调用
            triggerResult = runExecutor(triggerParam, address);
            logger.info("返回的状态码" + triggerResult.getCode());
        } else {
            triggerResult = new ReturnT<>(ReturnT.FAIL_CODE, null);
        }
    }

    private static ReturnT<String> runExecutor(TriggerParam triggerParam, String address) {
        ReturnT<String> runResult = null;
        try {
            // 获取用于远程调用客户端的对象
            ExecutorBiz executorBiz = XxlJobScheduler.getExecutorBiz(address);
            // 远程调用客户端，等待客户端返回数据
            runResult = executorBiz.run(triggerParam);
        } catch (Exception e) {
            logger.error(">>>>>>>>>>> xxl-job trigger error, please check if the executor[{}] is running.", address, e);
            runResult = new ReturnT<>(ReturnT.FAIL_CODE, ThrowableUtil.toString(e));
        }
        // 封装返回值
        StringBuffer runResultSb = new StringBuffer(I18nUtil.getString(I18nUtil.getString("jobconf_trigger_run") + "："));
        runResultSb.append("<br>address：").append(address);
        runResultSb.append("<br>code：").append(runResult.getCode());
        runResultSb.append("<br>msg：").append(runResult.getMsg());
        runResult.setMsg(runResultSb.toString());
        return runResult;
    }
}
