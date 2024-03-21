package com.moon.tinyxxljob.core.biz;

import com.moon.tinyxxljob.core.biz.model.HandleCallbackParam;
import com.moon.tinyxxljob.core.biz.model.RegistryParam;
import com.moon.tinyxxljob.core.biz.model.ReturnT;

import java.util.List;

/**
 * @author Chanmoey
 * Create at 2024-03-06
 */
public interface AdminBiz {

    ReturnT<String> registry(RegistryParam registryParam);

    ReturnT<String> registryRemove(RegistryParam registryParam);

    ReturnT<String> callback(List<HandleCallbackParam> callbackParamList);
}
