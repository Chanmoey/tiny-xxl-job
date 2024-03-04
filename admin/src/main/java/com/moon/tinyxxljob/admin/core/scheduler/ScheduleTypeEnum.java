package com.moon.tinyxxljob.admin.core.scheduler;

import com.moon.tinyxxljob.admin.core.util.I18nUtil;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author Chanmoey
 * Create at 2024-03-01
 */
public enum ScheduleTypeEnum {

    //不使用任何类型
    NONE(I18nUtil.getString("schedule_type_none")),

    //一般都是用cron表达式
    CRON(I18nUtil.getString("schedule_type_cron")),

    //按照固定频率
    FIX_RATE(I18nUtil.getString("schedule_type_fix_rate"));


    private String title;

    ScheduleTypeEnum(String title) {
        this.title = title;
    }

    public String getTitle() {
        return title;
    }

    private static final Map<String, ScheduleTypeEnum> CACHE = Arrays.stream(ScheduleTypeEnum.values()).collect(Collectors.toMap(ScheduleTypeEnum::name, Function.identity()));

    public static ScheduleTypeEnum match(String name, ScheduleTypeEnum defaultItem) {
        return CACHE.getOrDefault(name, defaultItem);
    }

}
