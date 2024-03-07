package com.moon.tinyxxljob.core.handler.annotation;

import java.lang.annotation.*;

/**
 * @author Chanmoey
 * Create at 2024-03-05
 */
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface XxlJob {

    // 定时任务的名称
    String value();

    // 初始化方法
    String init() default "";

    // 销毁方法
    String destroy() default "";
}
