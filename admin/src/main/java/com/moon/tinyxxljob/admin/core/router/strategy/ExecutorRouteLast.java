package com.moon.tinyxxljob.admin.core.router.strategy;



import com.moon.tinyxxljob.admin.core.router.ExecutorRouter;
import com.moon.tinyxxljob.core.biz.model.ReturnT;
import com.moon.tinyxxljob.core.biz.model.TriggerParam;

import java.util.List;


public class ExecutorRouteLast extends ExecutorRouter {

    @Override
    public ReturnT<String> route(TriggerParam triggerParam, List<String> addressList) {
        return new ReturnT<>(addressList.get(addressList.size() - 1));
    }

}
