package com.icngc.constant;

/**
 * @author: Mr.Wang
 * @create: 2018-11-26 20:05
 */
public class Constants {
    /**
     * 数据库链接信息共通的资源文件名
     */
    public static final String DBCP_COFIG_FILE = "dbcp.cofig.file";
    /**
     * 部署模式
     */
    public static final String SPARK_JOB_DEPLOY_MODE = "spark.job.run.mode";

    /**
     * 共通的初始化值
     */
    public static final String COMMON_INIT = "=0|";
    public static final String COMMON_INIT_2 = "=0";

    /**
     * 不同session数的标示(去重后的session总数)
     */
    public static final String SESSION_COUNT = "session_count";

    /**
     * 时长标识
     */
    public static final String TIME_PERIOD_1s_3s = "1s_3s";
    public static final String TIME_PERIOD_4s_6s = "4s_6s";
    public static final String TIME_PERIOD_7s_9s = "7s_9s";
    public static final String TIME_PERIOD_10s_30s = "10s_30s";
    public static final String TIME_PERIOD_30s_60s = "30s_60s";
    public static final String TIME_PERIOD_1m_3m = "1m_3m";
    public static final String TIME_PERIOD_3m_10m = "3m_10m";
    public static final String TIME_PERIOD_10m_30m = "10m_30m";
    public static final String TIME_PERIOD_30m = "30m";

    /**
     * 步长标识
     */
    public static final String STEP_PERIOD_1_3 = "1_3";
    public static final String STEP_PERIOD_4_6 = "4_6";
    public static final String STEP_PERIOD_7_9 = "7_9";
    public static final String STEP_PERIOD_10_30 = "10_30";
    public static final String STEP_PERIOD_30_60 = "30_60";
    public static final String STEP_PERIOD_60 = "60";

    /**
     * session聚合统计的结果常量
     *
     * session_count=0|1s_3s=0|4s_6s=0|...|60=0
     *
     * 设想：经过累加器不断操作后，值最终形如：
     *
     * session_count=100|1s_3s=4|4s_6s=3|...|60=9
     */
    public static StringBuilder AGGR_RESULT = new StringBuilder()
            .append(SESSION_COUNT).append(COMMON_INIT)
            .append(TIME_PERIOD_1s_3s).append(COMMON_INIT)
            .append(TIME_PERIOD_4s_6s).append(COMMON_INIT)
            .append(TIME_PERIOD_7s_9s).append(COMMON_INIT)
            .append(TIME_PERIOD_10s_30s).append(COMMON_INIT)
            .append(TIME_PERIOD_30s_60s).append(COMMON_INIT)
            .append(TIME_PERIOD_1m_3m).append(COMMON_INIT)
            .append(TIME_PERIOD_3m_10m).append(COMMON_INIT)
            .append(TIME_PERIOD_10m_30m).append(COMMON_INIT)
            .append(TIME_PERIOD_30m).append(COMMON_INIT)
            .append(STEP_PERIOD_1_3).append(COMMON_INIT)
            .append(STEP_PERIOD_4_6).append(COMMON_INIT)
            .append(STEP_PERIOD_7_9).append(COMMON_INIT)
            .append(STEP_PERIOD_10_30).append(COMMON_INIT)
            .append(STEP_PERIOD_30_60).append(COMMON_INIT)
            .append(STEP_PERIOD_60).append(COMMON_INIT_2);
}
