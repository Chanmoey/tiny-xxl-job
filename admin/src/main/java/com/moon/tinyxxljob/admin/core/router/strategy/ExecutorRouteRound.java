package com.moon.tinyxxljob.admin.core.router.strategy;

import com.moon.tinyxxljob.admin.core.router.ExecutorRouter;
import com.moon.tinyxxljob.core.biz.model.ReturnT;
import com.moon.tinyxxljob.core.biz.model.TriggerParam;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Chanmoey
 * Create at 2024-03-14
 */
public class ExecutorRouteRound extends ExecutorRouter {

    private static ConcurrentMap<Integer, AtomicInteger> routeCountEachJob = new ConcurrentHashMap<>();

    private static long cacheValidTime = 0;

    private static final Random r = new Random();

    @Override
    public ReturnT<String> route(TriggerParam triggerParam, List<String> addressList) {

        return new ReturnT<>(addressList.get(index(triggerParam.getJobId()) % addressList.size()));
    }


    private static int index(int jobId) {
        if (System.currentTimeMillis() > cacheValidTime) {
            routeCountEachJob.clear();
            cacheValidTime = System.currentTimeMillis() + 1000 * 60 * 60 * 24;
        }

        AtomicInteger count = routeCountEachJob.get(jobId);

        if (count == null || count.get() > 100000) {
            // 如果不设置随机值，那么第一次调用，永远是第一个
            // 如果很多任务同时执行，但是一开始，全都选第一个，就导致第一个执行器压力过大
            count = new AtomicInteger(r.nextInt(100));
        } else {
            count.addAndGet(1);
        }

        routeCountEachJob.put(jobId, count);

        return count.get();
    }
}
