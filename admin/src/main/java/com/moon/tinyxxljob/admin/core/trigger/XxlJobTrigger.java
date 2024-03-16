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
import com.moon.tinyxxljob.admin.core.model.XxlJobLog;
import com.moon.tinyxxljob.core.util.IpUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
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

        // 日志收集
        XxlJobLog jobLog = new XxlJobLog();
        //记录定时任务的执行器组id
        jobLog.setJobGroup(jobInfo.getJobGroup());
        //设置定时任务的id
        jobLog.setJobId(jobInfo.getId());
        //设置定时任务的触发时间
        jobLog.setTriggerTime(new Date());
        // TODO 同步写入库，在这里把定时任务日志保存到数据库中，保存成功之后，定时任务日志的id也就有了
        // 改为预先分配一个logId，然后异步写入？
        XxlJobAdminConfig.getAdminConfig().getXxlJobLogDao().save(jobLog);
        logger.debug(">>>>>>>>>>> xxl-job trigger start, jobId:{}", jobLog.getId());

        // 触发参数
        TriggerParam triggerParam = new TriggerParam();
        triggerParam.setJobId(jobInfo.getId());
        triggerParam.setExecutorHandler(jobInfo.getExecutorHandler());
        triggerParam.setExecutorParams(jobInfo.getExecutorParam());
        triggerParam.setExecutorBlockStrategy(jobInfo.getExecutorBlockStrategy());
        triggerParam.setLogId(jobLog.getId());
        triggerParam.setLogDateTime(jobLog.getTriggerTime().getTime());
        triggerParam.setGlueType(jobInfo.getGlueType());
        String address = null;
        ReturnT<String> routeAddressResult = null;
        List<String> registryList = group.getRegistryList();
        // 路由策略，选择一个地址
        if (registryList != null && !registryList.isEmpty()) {
            routeAddressResult = executorRouteStrategyEnum.getRouter().route(triggerParam, registryList);
            if (ReturnT.SUCCESS_CODE == routeAddressResult.getCode()) {
                address = routeAddressResult.getContent();
            } else {
                routeAddressResult = new ReturnT<>(ReturnT.FAIL_CODE, I18nUtil.getString("jobconf_trigger_address_empty"));
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

        //在这里拼接一下触发任务的信息，其实就是web界面的调度备注
        StringBuffer triggerMsgSb = new StringBuffer();
        triggerMsgSb.append(I18nUtil.getString("jobconf_trigger_type")).append("：").append(triggerType.getTitle());
        triggerMsgSb.append("<br>").append(I18nUtil.getString("jobconf_trigger_admin_adress")).append("：").append(IpUtil.getIp());
        triggerMsgSb.append("<br>").append(I18nUtil.getString("jobconf_trigger_exe_regtype")).append("：")
                .append((group.getAddressType() == 0) ? I18nUtil.getString("jobgroup_field_addressType_0") : I18nUtil.getString("jobgroup_field_addressType_1"));
        triggerMsgSb.append("<br>").append(I18nUtil.getString("jobconf_trigger_exe_regaddress")).append("：").append(group.getRegistryList());
        triggerMsgSb.append("<br>").append(I18nUtil.getString("jobinfo_field_executorRouteStrategy")).append("：").append(executorRouteStrategyEnum.getTitle());
        //注释的都是暂时用不上的
//        if (shardingParam != null) {
//            triggerMsgSb.append("("+shardingParam+")");
//        }
        //triggerMsgSb.append("<br>").append(I18nUtil.getString("jobinfo_field_executorBlockStrategy")).append("：").append(blockStrategy.getTitle());
        triggerMsgSb.append("<br>").append(I18nUtil.getString("jobinfo_field_timeout")).append("：").append(jobInfo.getExecutorTimeout());
        triggerMsgSb.append("<br>").append(I18nUtil.getString("jobinfo_field_executorFailRetryCount")).append("：").append(finalFailRetryCount);
        triggerMsgSb.append("<br><br><span style=\"color:#00c0ef;\" > >>>>>>>>>>>" + I18nUtil.getString("jobconf_trigger_run") + "<<<<<<<<<<< </span><br>")
                .append((routeAddressResult != null && routeAddressResult.getMsg() != null) ? routeAddressResult.getMsg() + "<br><br>" : "").append(triggerResult.getMsg() != null ? triggerResult.getMsg() : "");
        //设置执行器地址
        jobLog.setExecutorAddress(address);
        //设置执行定时任务的方法名称
        jobLog.setExecutorHandler(jobInfo.getExecutorHandler());
        //设置执行参数
        jobLog.setExecutorParam(jobInfo.getExecutorParam());
        //jobLog.setExecutorShardingParam(shardingParam);
        //设置失败重试次数
        jobLog.setExecutorFailRetryCount(finalFailRetryCount);
        //设置触发结果码
        jobLog.setTriggerCode(triggerResult.getCode());
        //设置触发任务信息，也就是调度备注
        jobLog.setTriggerMsg(triggerMsgSb.toString());
        //更新数据库信息
        XxlJobAdminConfig.getAdminConfig().getXxlJobLogDao().updateTriggerInfo(jobLog);
        logger.debug(">>>>>>>>>>> xxl-job trigger end, jobId:{}", jobLog.getId());
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
