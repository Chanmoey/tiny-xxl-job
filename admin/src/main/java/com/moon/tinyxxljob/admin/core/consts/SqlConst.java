package com.moon.tinyxxljob.admin.core.consts;

/**
 * @author Chanmoey
 * Create at 2024/2/26
 */
public class SqlConst {

    public static final String LOCK = "SELECT id FROM xxl_job_lock WHERE lock_name = 'schedule_lock' FOR UPDATE";
}
