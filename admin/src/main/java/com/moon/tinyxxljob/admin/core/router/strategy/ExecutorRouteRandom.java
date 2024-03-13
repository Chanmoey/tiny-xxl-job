package com.moon.tinyxxljob.admin.core.router.strategy;

import com.moon.tinyxxljob.admin.core.router.ExecutorRouter;
import com.moon.tinyxxljob.core.biz.model.ReturnT;
import com.moon.tinyxxljob.core.biz.model.TriggerParam;

import java.util.List;
import java.util.Random;

/**
 * @author Chanmoey
 * Create at 2024-03-14
 */
public class ExecutorRouteRandom extends ExecutorRouter {

    private static Random localRandom = new Random();


    @Override
    public ReturnT<String> route(TriggerParam triggerParam, List<String> addressList) {
        return new ReturnT<>(addressList.get(localRandom.nextInt(addressList.size())));
    }
}
