package com.moon.tinyxxljob.admin.core.conf;

import com.moon.tinyxxljob.admin.core.model.XxlJobGroup;
import com.moon.tinyxxljob.admin.core.scheduler.XxlJobScheduler;
import com.moon.tinyxxljob.admin.dao.XxlJobGroupDao;
import com.moon.tinyxxljob.admin.dao.XxlJobInfoDao;
import com.moon.tinyxxljob.admin.dao.XxlJobRegistryDao;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;

import javax.annotation.Resource;
import javax.sql.DataSource;
import java.sql.DatabaseMetaData;
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


    // TODO 数据库访问的dao
    @Resource
    private DataSource dataSource;

    @Resource
    private XxlJobInfoDao xxlJobInfoDao;

    @Resource
    private XxlJobGroupDao xxlJobGroup;

    public XxlJobRegistryDao getXxlJobRegistryDao() {
        return xxlJobRegistryDao;
    }

    public void setXxlJobRegistryDao(XxlJobRegistryDao xxlJobRegistryDao) {
        this.xxlJobRegistryDao = xxlJobRegistryDao;
    }

    @Resource
    private XxlJobRegistryDao xxlJobRegistryDao;

    public DataSource getDataSource() {
        return dataSource;
    }

    public XxlJobInfoDao getXxlJobInfoDao() {
        return xxlJobInfoDao;
    }

    public void setXxlJobInfoDao(XxlJobInfoDao xxlJobInfoDao) {
        this.xxlJobInfoDao = xxlJobInfoDao;
    }

    public XxlJobGroupDao getXxlJobGroupDao() {
        return xxlJobGroup;
    }

    public void setXxlJobGroupDao(XxlJobGroupDao xxlJobGroup) {
        this.xxlJobGroup = xxlJobGroup;
    }
}
