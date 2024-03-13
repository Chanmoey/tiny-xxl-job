package com.moon.tinyxxljob.admin.service.impl;

import com.moon.tinyxxljob.admin.core.thread.JobRegistryHelper;
import com.moon.tinyxxljob.core.biz.AdminBiz;
import com.moon.tinyxxljob.core.biz.model.RegistryParam;
import com.moon.tinyxxljob.core.biz.model.ReturnT;
import org.springframework.stereotype.Service;

/**
 * @author Chanmoey
 * Create at 2024-03-14
 */
@Service
public class AdminBizImpl implements AdminBiz {


    @Override
    public ReturnT<String> registry(RegistryParam registryParam) {
        return JobRegistryHelper.getInstance().registry(registryParam);
    }

    @Override
    public ReturnT<String> registryRemove(RegistryParam registryParam) {
        return JobRegistryHelper.getInstance().registryRemove(registryParam);
    }
}
