package com.moon.tinyxxljob.admin.core.conf;

import com.moon.tinyxxljob.admin.core.scheduler.XxlJobScheduler;
import com.moon.tinyxxljob.admin.dao.*;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.mail.javamail.JavaMailSender;

import javax.annotation.Resource;
import javax.sql.DataSource;
import java.util.Arrays;

/**
 * @author Chanmoey
 * Create at 2024/2/26
 */
public class XxlJobAdminConfig implements InitializingBean, DisposableBean {

    // 单例
    private static XxlJobAdminConfig adminConfig = null;

    public static XxlJobAdminConfig getAdminConfig() {
        return adminConfig;
    }

    private XxlJobScheduler xxlJobScheduler;

    @Override
    public void destroy() throws Exception {
        // 销毁一些资源
        xxlJobScheduler.destroy();
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        adminConfig = this;
        xxlJobScheduler = new XxlJobScheduler();
        xxlJobScheduler.init();
    }

    @Value("${xxl.job.i18n}")
    private String i18n;

    @Value("${xxl.job.accessToken}")
    private String accessToken;

    @Value("${spring.mail.from}")
    private String emailFrom;

    //快线程池的最大线程数
    @Value("${xxl.job.triggerpool.fast.max}")
    private int triggerPoolFastMax;

    //慢线程池的最大线程数
    @Value("${xxl.job.triggerpool.slow.max}")
    private int triggerPoolSlowMax;

    //该属性是日志保留时间的意思
    @Value("${xxl.job.logretentiondays}")
    private int logretentiondays;

    public String getI18n() {
        if (!Arrays.asList("zh_CN", "zh_TC", "en").contains(i18n)) {
            return "zh_CN";
        }
        return i18n;
    }

    public String getAccessToken() {
        return accessToken;
    }


    public String getEmailFrom() {
        return emailFrom;
    }

    public int getTriggerPoolFastMax() {
        if (triggerPoolFastMax < 200) {
            return 200;
        }
        return triggerPoolFastMax;
    }


    public int getTriggerPoolSlowMax() {
        if (triggerPoolSlowMax < 100) {
            return 100;
        }
        return triggerPoolSlowMax;
    }


    public int getLogretentiondays() {
        if (logretentiondays < 7) {
            return -1;
        }
        return logretentiondays;
    }


    @Resource
    private XxlJobLogDao xxlJobLogDao;
    @Resource
    private XxlJobInfoDao xxlJobInfoDao;
    @Resource
    private XxlJobRegistryDao xxlJobRegistryDao;
    @Resource
    private XxlJobGroupDao xxlJobGroupDao;
    @Resource
    private XxlJobLogReportDao xxlJobLogReportDao;
    @Resource
    private JavaMailSender mailSender;
    @Resource
    private DataSource dataSource;


    public XxlJobLogDao getXxlJobLogDao() {
        return xxlJobLogDao;
    }

    public XxlJobInfoDao getXxlJobInfoDao() {
        return xxlJobInfoDao;
    }

    public XxlJobRegistryDao getXxlJobRegistryDao() {
        return xxlJobRegistryDao;
    }

    public XxlJobGroupDao getXxlJobGroupDao() {
        return xxlJobGroupDao;
    }

    public XxlJobLogReportDao getXxlJobLogReportDao() {
        return xxlJobLogReportDao;
    }

    public JavaMailSender getMailSender() {
        return mailSender;
    }

    public DataSource getDataSource() {
        return dataSource;
    }
}
