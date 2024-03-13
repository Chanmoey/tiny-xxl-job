package com.moon.tinyxxljob.core.biz;

import com.moon.tinyxxljob.core.biz.model.IdleBeatParam;
import com.moon.tinyxxljob.core.biz.model.ReturnT;
import com.moon.tinyxxljob.core.biz.model.TriggerParam;

/**
 * @author Chanmoey
 * Create at 2024-02-28
 */
public interface ExecutorBiz {

    ReturnT<String> beat();

    ReturnT<String> idleBeat(IdleBeatParam idleBeatParam);

    ReturnT<String> run(TriggerParam triggerParam);
}
