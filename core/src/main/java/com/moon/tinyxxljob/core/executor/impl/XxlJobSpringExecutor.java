package com.moon.tinyxxljob.core.executor.impl;

import com.moon.tinyxxljob.core.executor.XxlJobExecutor;
import com.moon.tinyxxljob.core.handler.annotation.XxlJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.annotation.AnnotatedElementUtils;

import java.lang.reflect.Method;
import java.util.Map;

/**
 * @author Chanmoey
 * Create at 2024-03-10
 */
public class XxlJobSpringExecutor extends XxlJobExecutor implements ApplicationContextAware, SmartInitializingSingleton, DisposableBean {

    private static final Logger logger = LoggerFactory.getLogger(XxlJobSpringExecutor.class);

    @Override
    public void afterSingletonsInstantiated() {
        // 将所有的定时任务方法，注册到IJobHandler中
        initJobHandlerMethodRepository(applicationContext);

        try {
            // 启动内嵌Netty服务器
            super.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void destroy() {
        // 停止Netty服务器和每一个JobThread线程
        super.destroy();
    }

    private void initJobHandlerMethodRepository(ApplicationContext applicationContext) {
        if (applicationContext == null) {
            return;
        }
        // 获取IOC容器中所有初始化好的bean的名字，这里的后两个参数都为boolean类型
        // 第一个决定查到的对象是否允许为非单例的，传入false，意思为不获得非单例对象
        // 第二个意思是查找的对象是否允许为延迟初始化的，就是LazyInit的意思，参数为true，就是允许的意思
        String[] beanDefinitionNames = applicationContext.getBeanNamesForType(Object.class, false, true);

        for (String beanDefinitionName : beanDefinitionNames) {
            Object bean = applicationContext.getBean(beanDefinitionName);

            // 收集添加了XxlJob注解的Method
            Map<Method, XxlJob> annotatedMethod = null;
            try {
                annotatedMethod = MethodIntrospector.selectMethods(bean.getClass(),
                        (MethodIntrospector.MetadataLookup<XxlJob>) method -> AnnotatedElementUtils.findMergedAnnotation(method, XxlJob.class));

            } catch (Throwable ex) {
                logger.error("xxl-job method-jobHandler resolve error for bean[" + beanDefinitionName + "].", ex);
            }

            if (annotatedMethod == null || annotatedMethod.isEmpty()) {
                continue;
            }

            for (Map.Entry<Method, XxlJob> methodXxlJobEntry : annotatedMethod.entrySet()) {
                Method executeMethod = methodXxlJobEntry.getKey();
                XxlJob xxlJob = methodXxlJobEntry.getValue();
                // 将定时任务包装再JobHandler中
                registerJobHandler(xxlJob, bean, executeMethod);
            }
        }
    }


    private static ApplicationContext applicationContext;

    //该方法就是ApplicationContextAware接口中定义的方法
    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        XxlJobSpringExecutor.applicationContext = applicationContext;
    }

    public static ApplicationContext getApplicationContext() {
        return applicationContext;
    }
}
