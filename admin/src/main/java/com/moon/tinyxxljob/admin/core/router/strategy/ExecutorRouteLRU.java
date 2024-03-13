package com.moon.tinyxxljob.admin.core.router.strategy;

import com.moon.tinyxxljob.admin.core.router.ExecutorRouter;
import com.moon.tinyxxljob.core.biz.model.ReturnT;
import com.moon.tinyxxljob.core.biz.model.TriggerParam;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author Chanmoey
 * Create at 2024-03-14
 */
public class ExecutorRouteLRU extends ExecutorRouter {
    /**
     * key：任务id
     * value：{
     *
     * }
     */
    private static ConcurrentMap<Integer, LinkedHashMap<String, String>> jobLRUMap = new ConcurrentHashMap<>();
    //Map中数据的缓存时间
    private static long cacheValidTime = 0;

    public String route(int jobId, List<String> addressList) {
        //判断当前时间是否大于Map的缓存时间
        if (System.currentTimeMillis() > cacheValidTime) {
            //如果大于，则意味着数据过期了，清除即可
            jobLRUMap.clear();
            //重新设置数据缓存有效期
            cacheValidTime = System.currentTimeMillis() + 1000*60*60*24;
        }
        //根据定时任务id从jobLRUMap中获得对应的Map
        LinkedHashMap<String, String> lruItem = jobLRUMap.get(jobId);
        if (lruItem == null) {
            //accessOrder为true就是让LinkedHashMap按访问顺序迭代的意思
            //默认是使用插入顺序迭代
            //如果为null说明该定时任务是第一次执行，所以要初始化一个Map
            lruItem = new LinkedHashMap<>(16, 0.75f, true);
            //把Map放到jobLRUMap中
            jobLRUMap.putIfAbsent(jobId, lruItem);
        }
        //判断有没有新添加的执行器
        for (String address: addressList) {
            //如果有就把它加入到lruItem中
            if (!lruItem.containsKey(address)) {
                lruItem.put(address, address);
            }
        }
        //判断有没有过期的执行器
        List<String> delKeys = new ArrayList<>();
        for (String existKey: lruItem.keySet()) {
            if (!addressList.contains(existKey)) {
                delKeys.add(existKey);
            }
        }
        //有就把执行器删除
        if (!delKeys.isEmpty()) {
            for (String delKey: delKeys) {
                lruItem.remove(delKey);
            }
        }
        //使用迭代器得到第一个数据(缓存的节点，一旦被访问，就会放到链表尾部，所以第一个是最近最少访问的)
        String eldestKey = lruItem.entrySet().iterator().next().getKey();
        //得到对应的执行器地址
        //返回执行器地址
        return lruItem.get(eldestKey);
    }

    @Override
    public ReturnT<String> route(TriggerParam triggerParam, List<String> addressList) {
        String address = route(triggerParam.getJobId(), addressList);
        return new ReturnT<>(address);
    }
}
