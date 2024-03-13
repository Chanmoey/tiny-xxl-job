package com.moon.tinyxxljob.core.executor;

import com.moon.tinyxxljob.core.biz.AdminBiz;
import com.moon.tinyxxljob.core.biz.client.AdminBizClient;
import com.moon.tinyxxljob.core.handler.IJobHandler;
import com.moon.tinyxxljob.core.handler.annotation.XxlJob;
import com.moon.tinyxxljob.core.handler.impl.MethodJobHandler;
import com.moon.tinyxxljob.core.server.EmbedServer;
import com.moon.tinyxxljob.core.thread.JobThread;
import com.moon.tinyxxljob.core.util.IpUtil;
import com.moon.tinyxxljob.core.util.NetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 任务执行器
 *
 * @author Chanmoey
 * Create at 2024-03-06
 */
public class XxlJobExecutor {

    private static final Logger logger = LoggerFactory.getLogger(XxlJobExecutor.class);

    /**
     * 调度中心的地址
     */
    private String adminAddresses;

    /**
     * 访问Token
     */
    private String accessToken;

    /**
     * 执行器的名称
     */
    private String appname;

    /**
     * 执行器的地址：ip + 端口
     */
    private String address;

    /**
     * 执行器的ip地址
     */
    private String ip;

    /**
     * 端口号
     */
    private int port;

    /**
     * 执行器的日志收集地址
     */
    private String logPath;

    /**
     * 日志的保留天数，默认30天
     */
    private int logRetentionDays;

    public void setAdminAddresses(String adminAddresses) {
        this.adminAddresses = adminAddresses;
    }
    public void setAccessToken(String accessToken) {
        this.accessToken = accessToken;
    }
    public void setAppname(String appname) {
        this.appname = appname;
    }
    public void setAddress(String address) {
        this.address = address;
    }
    public void setIp(String ip) {
        this.ip = ip;
    }
    public void setPort(int port) {
        this.port = port;
    }
    public void setLogPath(String logPath) {
        this.logPath = logPath;
    }
    public void setLogRetentionDays(int logRetentionDays) {
        this.logRetentionDays = logRetentionDays;
    }

    public void start() throws Exception {
        // 根据用户配置的调度中心的地址，把用来远程注册的客户端初始化好
        initAdminBizList(adminAddresses, accessToken);
        // 启动执行器的内嵌服务器，一个Netty Http服务器
        // 把自己注册到所有的注册中心去
        initEmbedServer(address, ip, port, appname, accessToken);
    }

    public void destroy(){
        //首先停止内嵌服务器
        stopEmbedServer();
        //停止真正执行定时任务的各个线程
        if (!jobThreadRepository.isEmpty()) {
            for (Map.Entry<Integer, JobThread> item : jobThreadRepository.entrySet()) {
                JobThread oldJobThread = removeJobThread(item.getKey(), "web container destroy and kill the job.");
                if (oldJobThread != null) {
                    try {
                        oldJobThread.join();
                    } catch (InterruptedException e) {
                        logger.error(">>>>>>>>>>> xxl-job, JobThread destroy(join) error, jobId:{}", item.getKey(), e);
                    }
                }
            }
            jobThreadRepository.clear();
        }
        //清空缓存jobHandler的Map
        jobHandlerRepository.clear();
    }

    /**
     * 存放访问调度中心的客户端，主要用来发送注册信息
     */
    private static List<AdminBiz> adminBizList;

    private void initAdminBizList(String adminAddresses, String accessToken) throws Exception{

        if (adminAddresses == null) {
            throw new RuntimeException("Illegal adminAddresses: null");
        }

        adminAddresses = adminAddresses.trim();

        if (adminAddresses.isEmpty()) {
            throw new RuntimeException("Illegal adminAddresses: empty adminAddress");
        }


        for (String address : adminAddresses.split(",")) {
            if (address == null) {
                continue;
            }
            address = address.trim();
            if (address.isEmpty()) {
                continue;
            }

            AdminBiz adminBiz = new AdminBizClient(address, accessToken);

            if (adminBizList == null) {
                adminBizList = new ArrayList<>();
            }

            adminBizList.add(adminBiz);
        }
    }

    public static List<AdminBiz> getAdminBizList(){
        return adminBizList;
    }

    private EmbedServer embedServer = null;

    /**
     * 启动执行器的Netty服务器
     */
    private void initEmbedServer(String address, String ip, int port, String appname, String accessToken) {
        port = port > 0 ? port : NetUtil.findAvailablePort(9999);
        ip = (ip != null && !ip.trim().isEmpty()) ? ip : IpUtil.getIp();
        if (address == null || address.trim().isEmpty()) {
            String ipPortAddress = IpUtil.getIpPort(ip, port);
            address = "http://{ip_port}/".replace("{ip_port}", ipPortAddress);
        }

        if (accessToken == null || address.trim().isEmpty()) {
            logger.warn(">>>>>>>>>>> xxl-job accessToken is empty. To ensure system security, please set the accessToken.");
        }

        embedServer = new EmbedServer();
        embedServer.start(address, port, appname, accessToken);
    }

    private void stopEmbedServer() {
        if (embedServer != null) {
            try {
                embedServer.stop();
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    /**
     * 存放IJobHandler对象
     */
    private static ConcurrentMap<String, IJobHandler> jobHandlerRepository = new ConcurrentHashMap<>();

    public static IJobHandler loadJobHandler(String name){
        return jobHandlerRepository.get(name);
    }

    public static IJobHandler registerJobHandler(String name, IJobHandler jobHandler){
        logger.info(">>>>>>>>>>> xxl-job register jobHandler success, name:{}, jobHandler:{}", name, jobHandler);
        return jobHandlerRepository.put(name, jobHandler);
    }

    protected void registerJobHandler(XxlJob xxlJob, Object bean, Method executeMethod) {
        if (xxlJob == null) {
            return;
        }

        String name = xxlJob.value();

        Class<?> clazz = bean.getClass();

        String methodName = executeMethod.getName();

        if (name.trim().isEmpty()) {
            throw new RuntimeException("xxl-job method-jobhandler name invalid, for[" + clazz + "#" + methodName + "] .");
        }

        // 避免同名的Handler
        if (loadJobHandler(name) != null) {
            throw new RuntimeException("xxl-job jobHandler[" + name + "] naming conflicts.");
        }

        executeMethod.setAccessible(true);

        Method initMethod = null;
        Method destroyMethod = null;

        if (!xxlJob.init().trim().isEmpty()) {
            try {
                initMethod = clazz.getDeclaredMethod(xxlJob.init());
                initMethod.setAccessible(true);
            } catch (NoSuchMethodException e) {
                throw new RuntimeException("xxl-job method-jobHandler initMethod invalid, for[" + clazz + "#" + methodName + "] .");

            }
        }

        if (xxlJob.destroy().trim().length() > 0) {
            try {
                //如果有就使用反射获得
                destroyMethod = clazz.getDeclaredMethod(xxlJob.destroy());
                //设置可访问
                destroyMethod.setAccessible(true);
            } catch (NoSuchMethodException e) {
                throw new RuntimeException("xxl-job method-jobHandler destroyMethod invalid, for[" + clazz + "#" + methodName + "] .");
            }
        }

        // 将定时任务封装，并缓存到map中
        registerJobHandler(name, new MethodJobHandler(bean, executeMethod, initMethod, destroyMethod));
    }

    /**
     * 每一个定时任务对应一个ID，每个定时任务由一个线程来执行；如果定时任务原来就有线程在执行，则停掉原来的线程
     */
    private static ConcurrentMap<Integer, JobThread> jobThreadRepository = new ConcurrentHashMap<>();

    public static JobThread registerJobThread(int jobId, IJobHandler handler, String removeOldReason) {
        JobThread newJobThread = new JobThread(jobId, handler);
        newJobThread.start();
        logger.info(">>>>>>>>>>> xxl-job register JobThread success, jobId:{}, handler:{}", jobId, handler);

        JobThread oldJobThread = jobThreadRepository.put(jobId, newJobThread);

        if (oldJobThread != null) {
            oldJobThread.toStop(removeOldReason);
            oldJobThread.interrupt();
        }

        return newJobThread;
    }

    public static JobThread removeJobThread(int jobId, String removeOldReason) {
        JobThread oldJobThread = jobThreadRepository.remove(jobId);
        if (oldJobThread != null) {
            oldJobThread.toStop(removeOldReason);
            oldJobThread.interrupt();
            return oldJobThread;
        }
        return null;
    }

    public static JobThread loadJobThread(int jobId){
        return jobThreadRepository.get(jobId);
    }
}
