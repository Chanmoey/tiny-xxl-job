package com.moon.tinyxxljob.admin.core.router.strategy;

import com.moon.tinyxxljob.admin.core.router.ExecutorRouter;
import com.moon.tinyxxljob.core.biz.model.ReturnT;
import com.moon.tinyxxljob.core.biz.model.TriggerParam;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * @author Chanmoey
 * Create at 2024-03-14
 */
public class ExecutorRouteConsistentHash extends ExecutorRouter {

    private static final int VIRTUAL_NODE_NUM = 100;

    @Override
    public ReturnT<String> route(TriggerParam triggerParam, List<String> addressList) {
        String address = hashJob(triggerParam.getJobId(), addressList);
        return new ReturnT<>(address);
    }

    /**
     * 使用MD5散列，计算Hash值
     */
    private static long hash(String key) {
        MessageDigest md5;
        try {
            md5 = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 not supported", e);
        }
        md5.reset();
        byte[] keyBytes = null;
        try {
            keyBytes = key.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Unknown string :" + key, e);
        }
        md5.update(keyBytes);
        byte[] digest = md5.digest();
        long hashCode = ((long) (digest[3] & 0xFF) << 24)
                | ((long) (digest[2] & 0xFF) << 16)
                | ((long) (digest[1] & 0xFF) << 8)
                | (digest[0] & 0xFF);
        return hashCode & 0xffffffffL;
    }

    public String hashJob(int jobId, List<String> addressList) {
        // TODO，每一次调度，都要计算一次？如果缓存起来
        TreeMap<Long, String> addressRing = new TreeMap<>();
        // 为地址算Hash，然后放到TreeMap中
        for (String address : addressList) {
            for (int i = 0; i < VIRTUAL_NODE_NUM; i++) {
                //计算执行器地址的hash值
                long addressHash = hash("SHARD-" + address + "-NODE-" + i);
                //把地址hash值和地址放到TreeMap中
                addressRing.put(addressHash, address);
            }
        }

        long jobHash = hash(String.valueOf(jobId));
        SortedMap<Long, String> lastRing = addressRing.tailMap(jobHash);
        if (lastRing.isEmpty()) {
            return addressRing.firstEntry().getValue();
        }

        return lastRing.get(lastRing.firstKey());
    }
}
