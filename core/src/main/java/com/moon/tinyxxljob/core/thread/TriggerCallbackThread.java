package com.moon.tinyxxljob.core.thread;

import com.moon.tinyxxljob.core.biz.AdminBiz;
import com.moon.tinyxxljob.core.biz.model.HandleCallbackParam;
import com.moon.tinyxxljob.core.biz.model.ReturnT;
import com.moon.tinyxxljob.core.context.XxlJobContext;
import com.moon.tinyxxljob.core.context.XxlJobHelper;
import com.moon.tinyxxljob.core.enums.RegistryConfig;
import com.moon.tinyxxljob.core.executor.XxlJobExecutor;
import com.moon.tinyxxljob.core.log.XxlJobFileAppender;
import com.moon.tinyxxljob.core.util.FileUtil;
import com.moon.tinyxxljob.core.util.JdkSerializeTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author Chanmoey
 * Create at 2024-03-22
 */
public class TriggerCallbackThread {

    private static Logger logger = LoggerFactory.getLogger(TriggerCallbackThread.class);

    private static TriggerCallbackThread instance = new TriggerCallbackThread();

    public static TriggerCallbackThread getInstance() {
        return instance;
    }

    private LinkedBlockingQueue<HandleCallbackParam> callBackQueue = new LinkedBlockingQueue<>();

    public static void pushCallBack(HandleCallbackParam callback) {
        getInstance().callBackQueue.add(callback);
        logger.debug(">>>>>>>>>>> xxl-job, push callback request, logId:{}", callback.getLogId());
    }

    private Thread triggerCallbackThread;

    private Thread triggerRetryCallbackThread;

    private volatile boolean toStop = false;

    public void start() {

        // 判断回调调度中心的客户端是否存在
        if (XxlJobExecutor.getAdminBizList() == null) {
            logger.warn(">>>>>>>>>>> xxl-job, executor callback config fail, adminAddresses is null.");
            return;
        }

        triggerCallbackThread = new Thread(() -> {
            while (!toStop) {
                try {
                    HandleCallbackParam callback = getInstance().callBackQueue.take();
                    if (callback != null) {
                        List<HandleCallbackParam> callbackParamList = new ArrayList<>();
                        // 把待回调的数据，转移到callbackParamList中
                        int drainToNum = getInstance().callBackQueue.drainTo(callbackParamList);
                        callbackParamList.add(callback);
                        if (!callbackParamList.isEmpty()) {
                            doCallback(callbackParamList);
                        }
                    }
                } catch (Exception e) {
                    if (!toStop) {
                        logger.error(e.getMessage(), e);
                    }
                }
            }

            // 这里循环退出来了，线程要停止了，最后一次把所有数据都回调回去
            try {
                List<HandleCallbackParam> callbackParamList = new ArrayList<>();
                getInstance().callBackQueue.drainTo(callbackParamList);
                if (!callbackParamList.isEmpty()) {
                    doCallback(callbackParamList);
                }
            } catch (Exception e) {
                if (!toStop) {
                    logger.error(e.getMessage(), e);
                }
            }
            logger.info(">>>>>>>>>>> xxl-job, executor callback thread destroy.");
        });
        triggerCallbackThread.setDaemon(true);
        triggerCallbackThread.setName("xxl-job, executor TriggerCallbackThread");
        triggerCallbackThread.start();

        triggerRetryCallbackThread = new Thread(() -> {
            while (!toStop) {
                try {
                    // 重新回调一次
                    retryFailCallbackFile();
                } catch (Exception e) {
                    if (!toStop) {
                        logger.error(e.getMessage(), e);
                    }
                }

                try {
                    TimeUnit.SECONDS.sleep(RegistryConfig.BEAT_TIMEOUT);
                } catch (InterruptedException e) {
                    if (!toStop) {
                        logger.error(e.getMessage(), e);
                    }
                }
            }
            logger.info(">>>>>>>>>>> xxl-job, executor retry callback thread destroy.");
        });

        triggerRetryCallbackThread.setDaemon(true);
        triggerRetryCallbackThread.start();
    }


    public void toStop() {
        toStop = true;
        if (triggerCallbackThread != null) {
            triggerCallbackThread.interrupt();
            try {
                triggerCallbackThread.join();
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
        }
        if (triggerRetryCallbackThread != null) {
            triggerRetryCallbackThread.interrupt();
            try {
                triggerRetryCallbackThread.join();
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    private void doCallback(List<HandleCallbackParam> callbackParamList) {
        boolean callbackRet = false;
        for (AdminBiz adminBiz : XxlJobExecutor.getAdminBizList()) {
            try {
                ReturnT<String> callbackResult = adminBiz.callback(callbackParamList);
                if (callbackResult != null && ReturnT.SUCCESS_CODE == callbackResult.getCode()) {
                    //回调成功了，记录一下日志
                    callbackLog(callbackParamList, "<br>----------- xxl-job job callback finish.");
                    //回调标志置为true，说明回调成功了
                    callbackRet = true;
                    break;
                } else {
                    //回调失败了，记录一下日志
                    callbackLog(callbackParamList, "<br>----------- xxl-job job callback fail, callbackResult:" + callbackResult);
                }
            } catch (Exception e) {
                //回调出现异常了，记录一下日志
                callbackLog(callbackParamList, "<br>----------- xxl-job job callback error, errorMsg:" + e.getMessage());
            }
        }
        if (!callbackRet) {
            //这里就是回调失败了的意思，要把回调失败的数据存储到本地一个专门的文件当中，方便重试线程重新回调
            appendFailCallbackFile(callbackParamList);
        }
    }

    private void callbackLog(List<HandleCallbackParam> callbackParamList, String logContent) {
        for (HandleCallbackParam callbackParam : callbackParamList) {
            String logFileName = XxlJobFileAppender.makeLogFileName(new Date(callbackParam.getLogDateTim()), callbackParam.getLogId());
            // 设置上下文对象，因为打日志需要从上下文拿到数据
            XxlJobContext.setXxlJobContext(new XxlJobContext(
                    -1,
                    null,
                    logFileName,
                    -1,
                    -1));
            //记录信息到本地日志文件中
            XxlJobHelper.log(logContent);
        }
    }

    //设置回调失败日志的本地存储路径
    private static String failCallbackFilePath = XxlJobFileAppender.getLogPath().concat(File.separator).concat("callbacklog").concat(File.separator);

    //设置回调失败日志存储的文件名
    private static String failCallbackFileName = failCallbackFilePath.concat("xxl-job-callback-{x}").concat(".log");

    private void appendFailCallbackFile(List<HandleCallbackParam> callbackParamList) {
        //判空校验
        if (callbackParamList == null || callbackParamList.isEmpty()) {
            return;
        }
        //把回调数据序列化
        byte[] callbackParamListBytes = JdkSerializeTool.serialize(callbackParamList);
        //创建文件名，把x用当前时间戳替换
        File callbackLogFile = new File(failCallbackFileName.replace("{x}", String.valueOf(System.currentTimeMillis())));
        if (callbackLogFile.exists()) {
            for (int i = 0; i < 100; i++) {
                callbackLogFile = new File(failCallbackFileName.replace("{x}", String.valueOf(System.currentTimeMillis()).concat("-").concat(String.valueOf(i))));
                if (!callbackLogFile.exists()) {
                    break;
                }
            }
        }
        //把HandleCallbackParam存储到本地
        FileUtil.writeFileContent(callbackLogFile, callbackParamListBytes);
    }

    private void retryFailCallbackFile() {
        //得到文件夹路径
        File callbackLogPath = new File(failCallbackFilePath);
        if (!callbackLogPath.exists()) {
            return;
        }
        if (callbackLogPath.isFile()) {
            callbackLogPath.delete();
        }
        if (!(callbackLogPath.isDirectory() && callbackLogPath.list() != null && Objects.requireNonNull(callbackLogPath.list()).length > 0)) {
            return;
        }
        //遍历文件夹中的日志文件
        for (File callbaclLogFile : Objects.requireNonNull(callbackLogPath.listFiles())) {
            //读取日志信息
            byte[] callbackParamListBytes = FileUtil.readFileContent(callbaclLogFile);
            if (callbackParamListBytes == null || callbackParamListBytes.length < 1) {
                //如果文件是空的就删除
                callbaclLogFile.delete();
                continue;
            }
            //反序列化一下
            List<HandleCallbackParam> callbackParamList = (List<HandleCallbackParam>) JdkSerializeTool.deserialize(callbackParamListBytes, List.class);
            //删除文件
            callbaclLogFile.delete();
            //重新回调一次
            doCallback(callbackParamList);
        }

    }
}
