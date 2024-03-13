package com.moon.tinyxxljob.core.biz.client;

import com.moon.tinyxxljob.core.biz.ExecutorBiz;
import com.moon.tinyxxljob.core.biz.model.IdleBeatParam;
import com.moon.tinyxxljob.core.biz.model.ReturnT;
import com.moon.tinyxxljob.core.biz.model.TriggerParam;
import com.moon.tinyxxljob.core.biz.util.XxlJobRemotingUtil;

/**
 * @author Chanmoey
 * Create at 2024-02-29
 */
public class ExecutorBizClient implements ExecutorBiz {

    private String addressUrl ;
    private String accessToken;
    private int timeout = 3;

    public ExecutorBizClient(){}

    public ExecutorBizClient(String addressUrl, String accessToken) {
        this.addressUrl = addressUrl;
        this.accessToken = accessToken;
        if (!this.addressUrl.endsWith("/")) {
            this.addressUrl = this.addressUrl + "/";
        }
    }

    @Override
    public ReturnT<String> beat() {
        return null;
    }

    @Override
    public ReturnT<String> idleBeat(IdleBeatParam idleBeatParam) {
        return null;
    }

    @Override
    public ReturnT<String> run(TriggerParam triggerParam) {
        return XxlJobRemotingUtil.postBody(addressUrl + "run", accessToken, timeout, triggerParam, String.class);
    }
}
