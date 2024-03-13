package com.moon.tinyxxljob.admin.core.router.strategy;

import com.moon.tinyxxljob.admin.core.router.ExecutorRouter;
import com.moon.tinyxxljob.admin.core.scheduler.XxlJobScheduler;
import com.moon.tinyxxljob.admin.core.util.I18nUtil;
import com.moon.tinyxxljob.core.biz.ExecutorBiz;
import com.moon.tinyxxljob.core.biz.model.IdleBeatParam;
import com.moon.tinyxxljob.core.biz.model.ReturnT;
import com.moon.tinyxxljob.core.biz.model.TriggerParam;

import java.util.List;

/**
 * 忙碌转移策略
 *
 * @author Chanmoey
 * Create at 2024-03-14
 */
public class ExecutorRouteBusyover extends ExecutorRouter {

    @Override
    public ReturnT<String> route(TriggerParam triggerParam, List<String> addressList) {
        StringBuilder sb = new StringBuilder();
        for (String address : addressList) {
            ReturnT<String> idleBeatResult = null;
            try {
                ExecutorBiz executorBiz = XxlJobScheduler.getExecutorBiz(address);
                idleBeatResult = executorBiz.idleBeat(new IdleBeatParam(triggerParam.getJobId()));
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                idleBeatResult = new ReturnT<>(ReturnT.FAIL_CODE, e.toString());
            }

            sb.append((!sb.isEmpty()) ? "<br><br>" : "")
                    .append(I18nUtil.getString("jobconf_idleBeat")).append("：")
                    .append("<br>address：").append(address)
                    .append("<br>code：").append(idleBeatResult.getCode())
                    .append("<br>msg：").append(idleBeatResult.getMsg());
            if (ReturnT.SUCCESS_CODE == idleBeatResult.getCode()) {
                idleBeatResult.setMsg(sb.toString());
                idleBeatResult.setContent(address);
                return idleBeatResult;
            }
        }

        return new ReturnT<>(ReturnT.FAIL_CODE, sb.toString());

    }
}
