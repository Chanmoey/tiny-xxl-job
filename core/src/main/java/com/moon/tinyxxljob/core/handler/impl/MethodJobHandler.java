package com.moon.tinyxxljob.core.handler.impl;

import com.moon.tinyxxljob.core.handler.IJobHandler;

import java.lang.reflect.Method;

/**
 * 负责通过反射调用定时任务
 *
 * @author Chanmoey
 * Create at 2024-03-05
 */
public class MethodJobHandler implements IJobHandler {

    /**
     * 目标对象，用户定义的IOC容器中的Bean，承载定时任务的类
     */
    private final Object target;

    /**
     * 目标方法，定时任务本身
     */
    private final Method method;

    /**
     * bean对象的初始化方法
     */
    private Method initMethod;

    /**
     * bean对象的销毁方法
     */
    private Method destroyMethod;

    public MethodJobHandler(Object target, Method method, Method initMethod, Method destroyMethod) {
        this.target = target;
        this.method = method;
        this.initMethod = initMethod;
        this.destroyMethod = destroyMethod;
    }

    @Override
    public void execute() throws Exception {
        Class<?>[] paramTypes = this.method.getParameterTypes();
        if (paramTypes.length > 0) {
            this.method.invoke(target, new Object[paramTypes.length]);
        } else {
            method.invoke(target);
        }
    }

    @Override
    public void init() throws Exception {
        if (initMethod != null) {
            initMethod.invoke(target);
        }
    }

    @Override
    public void destroy() throws Exception {
        if (destroyMethod != null) {
            destroyMethod.invoke(target);
        }
    }

    @Override
    public String toString() {
        return super.toString()+"["+ target.getClass() + "#" + method.getName() +"]";
    }
}
