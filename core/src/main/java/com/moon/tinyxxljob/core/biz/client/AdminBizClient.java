package com.moon.tinyxxljob.core.biz.client;

import com.moon.tinyxxljob.core.biz.AdminBiz;
import com.moon.tinyxxljob.core.biz.model.HandleCallbackParam;
import com.moon.tinyxxljob.core.biz.model.RegistryParam;
import com.moon.tinyxxljob.core.biz.model.ReturnT;
import com.moon.tinyxxljob.core.biz.util.XxlJobRemotingUtil;

import java.util.List;

/**
 * @author Chanmoey
 * Create at 2024-03-06
 */
public class AdminBizClient implements AdminBiz {
    public AdminBizClient() {
    }

    public AdminBizClient(String addressUrl, String accessToken) {
        this.addressUrl = addressUrl;
        this.accessToken = accessToken;
        if (!this.addressUrl.endsWith("/")) {
            this.addressUrl = this.addressUrl + "/";
        }
    }

    //这里的地址是调度中心的服务地址
    private String addressUrl ;
    //token令牌，执行器和调度中心两端要一致
    private String accessToken;
    //访问超时时间
    private int timeout = 3;



    @Override
    public ReturnT<String> registry(RegistryParam registryParam) {
        return XxlJobRemotingUtil.postBody(addressUrl + "api/registry", accessToken, timeout, registryParam, String.class);
    }


    @Override
    public ReturnT<String> registryRemove(RegistryParam registryParam) {
        return XxlJobRemotingUtil.postBody(addressUrl + "api/registryRemove", accessToken, timeout, registryParam, String.class);
    }

    @Override
    public ReturnT<String> callback(List<HandleCallbackParam> callbackParamList) {
        return XxlJobRemotingUtil.postBody(addressUrl+"api/callback", accessToken, timeout, callbackParamList, String.class);
    }
}
