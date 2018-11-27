package com.icngc.jobs.session

import java.text.SimpleDateFormat
import java.util

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.icngc.bean.{SessionAggrStat, TaskParam}
import com.icngc.constant.Constants
import com.icngc.dao.common.ITaskDao
import com.icngc.dao.impl.ITaskDaoImpl
import com.icngc.dao.session.ISessionAggrStat
import com.icngc.dao.session.impl.SessionAggrStatImpl
import com.icngc.mock.MockData
import com.icngc.utils.{ResourcesUtils, StringUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}


/**
  * 用户session分析模块
  *
  * @author: Mr.Wang
  * @create: 2018-11-26 22:28
  */
object UserSessionAnalysisJob {


  /**
    * 自定义计算时长的函数
    * @param endTime
    * @param startTime
    * @return
    */
  def getTimeLen(endTime: String, startTime: String): Long = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    sdf.parse(endTime).getTime - sdf.parse(startTime).getTime
  }

  def main(args: Array[String]): Unit = {
    // 前提
    val spark: SparkSession = readyToOperate(args)

    // 步骤
    // 1. 按条件筛选session
    filterSessionsByCriteria(spark, args)
    // 2. 统计出符合条件的session中，访问时长在1s~3s、4s~6s、7s~9s、10s~30s、30s~60s、1m~3m、3m~10m、
    //  10m~30m、30m以上各个范围内的session占比；访问步长在1~3、4~6、7~9、10~30、30~60、60以上各个
    //  范围内的session占比
    getStepLenAndTimeRate(spark, args)
    // 3. 在符合条件的session中，按照时间比例随机抽取1000个session
    // 3.1 准备一个容器，用于存储session_id
//    val container: ArrayBuffer[String] = new ArrayBuffer
    // 3.2 求出每个时间段内的session数占总session数的比例值(不去重的seesion数)
    val totalSessionCnt = spark.sql("select count(*) totalSessionCnt from filter_after_action").first().getAs[Long]("totalSessionCnt")
    // 3.3 根据比例值rdd，从指定的时间段内抽取相应数量的session,并变形后保存到db中
    val rdd = spark.sql("select substring(action_time,1,13) timePeriod, count(*)/" + totalSessionCnt.toDouble + "rateValue  from filter_after_action group by substring(action_time,1,13)").rdd
    rdd.foreach(row => {
      // 循环分析rdd，每循环一次
      // 根据比率值从filter_after_action抽取session
      val nowTimePeriod = row.getAs[String]("timePeriod")
      val rateValue = row.getAs[Double]("rateValue")
      val needTotalSessionCnt = if(totalSessionCnt>1000) 1000 else totalSessionCnt
      val arr:Array[Row]=spark.sql("select session_id,action_time,search_keyword from filter_after_action where instr(action_time,'" + nowTimePeriod + "') >0").rdd.takeSample(true,(needTotalSessionCnt*rateValue).toInt)

      val rdd:RDD[Row] = spark.sparkContext.parallelize(arr)
      val structType:StructType = StructType(Seq(StructField("session_id",StringType,false),StructField("action_time",StringType,false),StructField("search_keyword",StringType,true)))

      spark.createDataFrame(rdd,structType).createOrReplaceTempView("temp_random")

      // 将结果映射为一张临时表，聚合后保存到db中
      val nowPeriodAllSessionRDD:RDD[Row] = spark.sql(" select  session_id,concat_ws(',', collect_set(distinct search_keyword)) search_keywords ,min(action_time),max(action_time) from temp_random group by session_id").rdd


      // todo 3.4 向存储中随机抽取出来的session的明细表中存取数据
      // 容器中存取的session_id与filter_after_action表进行内链接查询，查询处满足条件的记录保存到明细表中
    })
  }

  /**
    * 准备操作
    *
    * @param args
    * @return
    */
  def readyToOperate(args: Array[String]) = {
    // 0. 拦截非法操作
    if (args == null || args.length != 1) {
      // spark-submit 主类 jar taskid
      print("参数录入错误或参数为空")
    }
    // 1. SparkSession的实例(⚠️：若分析的是hive表，需要启用对hive的支持,Builder的实例.enableHiveSupport())
    val builder = SparkSession.builder().appName(UserSessionAnalysisJob.getClass.getSimpleName)

    // 若是本地集群模式，需要单独设置
    if (ResourcesUtils.dMode.toString.toLowerCase().equals("local")) {
      builder.master("local[*]")
    }

    val spark = builder.getOrCreate()

    // 2. 将模拟的数据装在进内存（hive表中数据）
    MockData.mock(spark.sparkContext, spark.sqlContext)

    // 3.设置日志的级别
    spark.sparkContext.setLogLevel("WARN")

    // 模拟数据测试
    //    spark.sql("select * from user_visit_action").show(1)
    spark
  }

  /**
    * 按条件筛选session
    *
    * @param spark
    * @param args
    */
  def filterSessionsByCriteria(spark: SparkSession, args: Array[String]) = {
    // 1.1 准备一个字符串构建器StringBuffer，用于存储sql
    val buffer = new StringBuffer()
    buffer.append("select * from user_visit_action u,user_info i where u.user_id=i.user_id")
    // 1.2 根据从mysql中task表中的字段task_param查询到的值，进行sql语句的拼接
    val taskId = args(0).toInt
    val taskDao: ITaskDao = new ITaskDaoImpl
    val task = taskDao.findTaskById(taskId)

    // 1.3 获取Task实体类中的task_param,task_param为json格式
    val taskParamJsonStr = task.getTask_param

    // 1.4 使用FastJson，将json对象格式的数据封装到实体类TaskParam中
    val taskParam: TaskParam = JSON.parseObject(taskParamJsonStr, classOf[TaskParam])
    // 1.5 获得参数
    val ages: util.List[Integer] = taskParam.getAges
    val genders: util.List[String] = taskParam.getGenders
    val professionals: util.List[String] = taskParam.getProfessionals
    val cities: util.List[String] = taskParam.getCities
    // 1.6 拼接字符串
    if (ages != null || ages.size() > 0) {
      buffer.append(" and i.age between ")
        .append(ages.get(0))
        .append(" and ")
        .append(ages.get(1))
    }

    if (genders != null || genders.size() > 0) {
      buffer.append(" and i.sex in(")
        .append(JSON.toJSONString(genders, SerializerFeature.UseSingleQuotes).replace("[", "").replace("]", ""))
        .append(")")
    }

    if (professionals != null || professionals.size() > 0) {
      buffer.append(" and i.professional in(")
        .append(JSON.toJSONString(professionals, SerializerFeature.UseSingleQuotes).replace("[", "").replace("]", ""))
        .append(")")
    }

    if (cities != null || cities.size() > 0) {
      buffer.append(" and i.city in(")
        .append(JSON.toJSONString(cities, SerializerFeature.UseSingleQuotes).replace("[", "").replace("]", ""))
        .append(")")
    }

    // 1.7 测试，然后将结果注册为一张临时表，供后续的步骤使用（为了提高速度，需要将临时表存起来）
    // 测试
    //    println("sql语句为" + buffer.toString)
    //    spark.sql(buffer.toString).show(2000)
    spark.sql(buffer.toString).createOrReplaceTempView("filter_after_action")
    spark.sqlContext.cacheTable("filter_after_action")
    spark.sql("select * from filter_after_action").show(1000)
  }

  /**
    * 统计出符合条件的session中，访问时长在1s~3s、4s~6s、7s~9s、10s~30s、30s~60s、1m~3m、3m~10m、
    * 10m~30m、30m以上各个范围内的session占比；访问步长在1~3、4~6、7~9、10~30、30~60、60以上各个
    * 范围内的session占比
    *
    * @param spark
    * @param args
    */
  def getStepLenAndTimeRate(spark: SparkSession, args: Array[String]) = {
    // 2.1 根据session_id进行分组，求出各个session的步长和时长
    //   2.1.1 注册自定义函数
    spark.udf.register("getTimeLen", (endTime: String, startTime: String) => getTimeLen(endTime, startTime))
    //   2.1.2 准备一个自定义累加器的实例，并进行注册
    val accumulator: SessionAggrStatAccumulator = new SessionAggrStatAccumulator
    spark.sparkContext.register(accumulator)
    // 2.2 将结果转换成rdd，循环分析rdd
    val rdd: RDD[Row] = spark.sql("select count(*) stepLen,getTimeLen(max(action_time),min(action_time)) timeLen from filter_after_action group by session_id").rdd
    rdd.collect.foreach(row => {
      // session_count累加 1 统计有多少个不重复的session_id
      accumulator.add(Constants.SESSION_COUNT)

      // 求session中相应步长的个数
      calStepLenSessionCnt(row, accumulator)

      // 求session中相应的时长的个数
      calTimeSessionCnt(row, accumulator)
    })
    // 2.3 将最终的结果保存到db的session_aggr_stat表
    saveSessionAggrStatToDB(accumulator, args)
  }

  /**
    * 求session中相应步长的个数
    *
    * @param row
    * @param accumulator
    */
  def calStepLenSessionCnt(row: Row, accumulator: SessionAggrStatAccumulator) = {
    val nowStepLen = row.getAs[Long]("stepLen")
    if (nowStepLen >= 1 && nowStepLen <= 3) {
      accumulator.add(Constants.STEP_PERIOD_1_3)
    } else if (nowStepLen >= 4 && nowStepLen <= 6) {
      accumulator.add(Constants.STEP_PERIOD_4_6)
    } else if (nowStepLen >= 7 && nowStepLen <= 9) {
      accumulator.add(Constants.STEP_PERIOD_7_9)
    } else if (nowStepLen >= 10 && nowStepLen <= 30) {
      accumulator.add(Constants.STEP_PERIOD_10_30)
    } else if (nowStepLen > 30 && nowStepLen <= 60) {
      accumulator.add(Constants.STEP_PERIOD_30_60)
    } else if (nowStepLen > 60) {
      accumulator.add(Constants.STEP_PERIOD_60)
    }
  }

  /**
    * 求session中相应的时长的个数
    *
    * @param row
    * @param accumulator
    */
  def calTimeSessionCnt(row: Row, accumulator: SessionAggrStatAccumulator) = {
    val nowTimeLen = row.getAs[Long]("timeLen")
    val timeLenSeconds = nowTimeLen / 1000
    val timeLenMinutes = timeLenSeconds / 60
    if (timeLenSeconds >= 1 && timeLenSeconds <= 3) {
      accumulator.add(Constants.TIME_PERIOD_1s_3s)
    } else if (timeLenSeconds >= 4 && timeLenSeconds <= 6) {
      accumulator.add(Constants.TIME_PERIOD_4s_6s)
    } else if (timeLenSeconds >= 7 && timeLenSeconds <= 9) {
      accumulator.add(Constants.TIME_PERIOD_7s_9s)
    } else if (timeLenSeconds >= 10 && timeLenSeconds <= 30) {
      accumulator.add(Constants.TIME_PERIOD_10s_30s)
    } else if (timeLenSeconds > 30 && timeLenSeconds < 60) {
      accumulator.add(Constants.TIME_PERIOD_30s_60s)
    } else if (timeLenMinutes >= 1 && timeLenMinutes < 3) {
      accumulator.add(Constants.TIME_PERIOD_1m_3m)
    } else if (timeLenMinutes >= 4 && timeLenMinutes <= 6) {
      accumulator.add(Constants.TIME_PERIOD_3m_10m)
    } else if (timeLenMinutes >= 10 && timeLenMinutes <= 30) {
      accumulator.add(Constants.TIME_PERIOD_10m_30m)
    } else if (timeLenMinutes >= 30) {
      accumulator.add(Constants.TIME_PERIOD_30m)
    }
  }

  /**
    * 将最终的结果保存到db的session_aggr_stat表
    *
    * @param accumulator
    * @param args
    */
  def saveSessionAggrStatToDB(accumulator: SessionAggrStatAccumulator, args: Array[String]) = {
    val finalResult = accumulator.value
    val session_count = StringUtils.getFieldFromConcatString(finalResult, "\\|", Constants.SESSION_COUNT).toInt
    val period_1s_3s = StringUtils.getFieldFromConcatString(finalResult, "\\|", Constants.TIME_PERIOD_1s_3s).toDouble
    val period_4s_6s = StringUtils.getFieldFromConcatString(finalResult, "\\|", Constants.TIME_PERIOD_4s_6s).toDouble
    val period_7s_9s = StringUtils.getFieldFromConcatString(finalResult, "\\|", Constants.TIME_PERIOD_7s_9s).toDouble
    val period_10s_30s = StringUtils.getFieldFromConcatString(finalResult, "\\|", Constants.TIME_PERIOD_10s_30s).toDouble
    val period_30s_60s = StringUtils.getFieldFromConcatString(finalResult, "\\|", Constants.TIME_PERIOD_30s_60s).toDouble
    val period_1m_3m = StringUtils.getFieldFromConcatString(finalResult, "\\|", Constants.TIME_PERIOD_1m_3m).toDouble
    val period_3m_10m = StringUtils.getFieldFromConcatString(finalResult, "\\|", Constants.TIME_PERIOD_3m_10m).toDouble
    val period_10m_30m = StringUtils.getFieldFromConcatString(finalResult, "\\|", Constants.TIME_PERIOD_10m_30m).toDouble
    val period_30m = StringUtils.getFieldFromConcatString(finalResult, "\\|", Constants.TIME_PERIOD_30m).toDouble
    val step_1_3 = StringUtils.getFieldFromConcatString(finalResult, "\\|", Constants.STEP_PERIOD_1_3).toDouble
    val step_4_6 = StringUtils.getFieldFromConcatString(finalResult, "\\|", Constants.STEP_PERIOD_4_6).toDouble
    val step_7_9 = StringUtils.getFieldFromConcatString(finalResult, "\\|", Constants.STEP_PERIOD_7_9).toDouble
    val step_10_30 = StringUtils.getFieldFromConcatString(finalResult, "\\|", Constants.STEP_PERIOD_10_30).toDouble
    val step_30_60 = StringUtils.getFieldFromConcatString(finalResult, "\\|", Constants.STEP_PERIOD_30_60).toDouble
    val step_60 = StringUtils.getFieldFromConcatString(finalResult, "\\|", Constants.STEP_PERIOD_60).toDouble
    val sessionAggrStatBean = new SessionAggrStat(args(0).toInt, session_count, period_1s_3s, period_4s_6s, period_7s_9s, period_10s_30s, period_30s_60s, period_1m_3m, period_3m_10m, period_10m_30m, period_30m, step_1_3, step_4_6, step_7_9, step_10_30, step_30_60, step_60)

    println(finalResult)
    val dao: ISessionAggrStat = new SessionAggrStatImpl
    dao.saveBeanToDB(sessionAggrStatBean)
  }
}
