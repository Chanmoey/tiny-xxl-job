package com.moon.tinyxxljob.admin.core.scheduler;

import com.moon.tinyxxljob.admin.core.util.I18nUtil;

import java.util.HashMap;
import java.util.Map;

/**
 * 定时任务调度错过后的策略
 *
 * @author Chanmoey
 * Create at 2024/2/27
 */
public enum MisfireStrategyEnum {

    //默认什么也不做
    DO_NOTHING(I18nUtil.getString("misfire_strategy_do_nothing")),

    //失败后重试一次
    FIRE_ONCE_NOW(I18nUtil.getString("misfire_strategy_fire_once_now"));

    private String title;

    MisfireStrategyEnum(String title) {
        this.title = title;
    }

    public String getTitle() {
        return title;
    }

    private static final Map<String, MisfireStrategyEnum> CACHE = new HashMap<>();

    static {
        for (MisfireStrategyEnum item: MisfireStrategyEnum.values()) {
            CACHE.put(item.name(), item);
        }
    }

    public static MisfireStrategyEnum match(String name, MisfireStrategyEnum defaultItem){
        return CACHE.getOrDefault(name, defaultItem);
    }

    public boolean fireOneNow() {
        return MisfireStrategyEnum.FIRE_ONCE_NOW == this;
    }
}
