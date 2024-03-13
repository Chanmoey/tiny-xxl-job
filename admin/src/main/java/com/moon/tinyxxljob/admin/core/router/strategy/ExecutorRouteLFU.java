package com.moon.tinyxxljob.admin.core.router.strategy;

import com.moon.tinyxxljob.admin.core.router.ExecutorRouter;
import com.moon.tinyxxljob.core.biz.model.ReturnT;
import com.moon.tinyxxljob.core.biz.model.TriggerParam;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author Chanmoey
 * Create at 2024-03-14
 */
public class ExecutorRouteLFU extends ExecutorRouter {
    /**
     * key：定时任务Id
     * value：{
     *     key：地址
     *     value：使用次数
     * }
     */
    private static ConcurrentMap<Integer, HashMap<String, Integer>> jobLfuMap = new ConcurrentHashMap<>();

    /**
     * 缓存有效时间
     */
    private static long cacheValidTime = 0;

    private static final Random r = new Random();


    public String route(int jobId, List<String> addressList) {

        // 如果缓存过期，则清空缓存
        if (System.currentTimeMillis() > cacheValidTime) {
            //如果大于，则意味着数据过期了，清除即可
            jobLfuMap.clear();
            //重新设置数据缓存有效期
            cacheValidTime = System.currentTimeMillis() + 1000*60*60*24;
        }

        HashMap<String, Integer> lfuItemMap = jobLfuMap.get(jobId);

        if (lfuItemMap == null) {
            //如果value为空，则创建一个Map
            lfuItemMap = new HashMap<>();
            //把Map添加到jobLfuMap中
            jobLfuMap.putIfAbsent(jobId, lfuItemMap);   // 避免重复覆盖
        }

        // 对所有地址进行初始化
        for (String address: addressList) {
            //首先是判断该执行器地址是不是第一次使用，如果是第一次使用，lfuItemMap里面肯定还没有数据
            //所以就要为第一次使用的执行器初始化一个对应的使用次数
            if (!lfuItemMap.containsKey(address) || lfuItemMap.get(address) >1000000 ) {
                //初始化操作在这里，因为这个路由策略说到底也是根据最不常用的次数来选择执行器的，所以，这里即便给执行器初始化了一个使用次数，最后还要判断一下哪个最不常用
                //才选择哪个。这里做了一个随机数选择，从0到执行器集合的长度之间随机选一个数作为执行器对应的使用次数
                //当然，当执行器使用次数大于1000000也会触发这个初始化操作
                //因为我添加注释的时候，先添加的那个类，所以详细注释也先写在那里面了，这里就不重复了
                lfuItemMap.put(address, r.nextInt(addressList.size()));
            }
        }

        //判断有没有过期的执行器
        List<String> delKeys = new ArrayList<>();
        for (String existKey: lfuItemMap.keySet()) {
            if (!addressList.contains(existKey)) {
                delKeys.add(existKey);
            }
        }

        // 删除过期的地址
        if (!delKeys.isEmpty()) {
            for (String delKey: delKeys) {
                lfuItemMap.remove(delKey);
            }
        }


        List<Map.Entry<String, Integer>> lfuItemList = new ArrayList<>(lfuItemMap.entrySet());
        //将lfuItemList中的数据按照执行器的使用次数做排序
        // TODO 换成优先队列会不会更好，自带排序
        lfuItemList.sort(Map.Entry.comparingByValue());
        //获取到的第一个就是使用次数最少的执行器
        Map.Entry<String, Integer> addressItem = lfuItemList.get(0);
        //因为要是用它了，所以把执行器的使用次数加1
        addressItem.setValue(addressItem.getValue() + 1);
        //返回执行器地址
        return addressItem.getKey();
    }

    @Override
    public ReturnT<String> route(TriggerParam triggerParam, List<String> addressList) {
        String address = route(triggerParam.getJobId(), addressList);
        return new ReturnT<>(address);
    }
}
