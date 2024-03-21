package com.moon.tinyxxljob.core.context;

/**
 * @author Chanmoey
 * Create at 2024-03-22
 */
public class XxlJobContext {

    public static final int HANDLE_CODE_SUCCESS = 200;
    public static final int HANDLE_CODE_FAIL = 500;
    public static final int HANDLE_CODE_TIMEOUT = 502;

    private final long jobId;

    private final String jobParam;

    private final String jobLogFileName;

    private final int shardIndex;

    private final int shardTotal;

    private int handleCode;

    private String handleMsg;

    public XxlJobContext(long jobId, String jobParam, String jobLogFileName, int shardIndex, int shardTotal) {
        this.jobId = jobId;
        this.jobParam = jobParam;
        this.jobLogFileName = jobLogFileName;
        this.shardIndex = shardIndex;
        this.shardTotal = shardTotal;
        //构造方法中唯一值得注意的就是这里，创建XxlJobContext对象的时候默认定时任务的执行结果就是成功
        //如果执行失败了，自由其他方法把这里设置成失败
        this.handleCode = HANDLE_CODE_SUCCESS;
    }

    public long getJobId() {
        return jobId;
    }

    public String getJobParam() {
        return jobParam;
    }

    public String getJobLogFileName() {
        return jobLogFileName;
    }

    public int getShardIndex() {
        return shardIndex;
    }

    public int getShardTotal() {
        return shardTotal;
    }

    public void setHandleCode(int handleCode) {
        this.handleCode = handleCode;
    }

    public int getHandleCode() {
        return handleCode;
    }

    public void setHandleMsg(String handleMsg) {
        this.handleMsg = handleMsg;
    }

    public String getHandleMsg() {
        return handleMsg;
    }

    private static InheritableThreadLocal<XxlJobContext> contextHolder = new InheritableThreadLocal<>();

    public static void setXxlJobContext(XxlJobContext xxlJobContext) {
        contextHolder.set(xxlJobContext);
    }

    public static XxlJobContext getXxlJobContext() {
        return contextHolder.get();
    }
}
