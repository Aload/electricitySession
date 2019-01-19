package datasource

import java.util
import java.util.{Arrays, UUID}

import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{DataFrame, Row, RowFactory, SQLContext}
import utils.{DateUtils, MathStringUtils}

import scala.util.Random

object DataPool {

  def mock(sc: SparkContext, sqlContext: SQLContext): Unit = {
    val rows = List[Row]()
    val keyWord = Array[String]("火锅", "蛋糕", "重庆辣子鸡", "重庆小面", "呷哺呷哺", "新辣道鱼火锅", "国贸大厦", "太古商场", "日本料理", "温泉")
    val date: String = DateUtils.getTodayDate()
    val actions = Array[String]("search", "click", "order", "pay")
    val randow: Random = new Random()
    var count: Int = 0
    while (count < 100) {
      count = count + 1
      val usrId = randow.nextInt(100)
      for (j <- 1 to 10) {
        val sessionId = UUID.randomUUID().toString.replace("-", "")
        val baseActionTime = date + "" + randow.nextInt(23)
        for (k <- 1 to 100) {
          val pageId = randow.nextInt(10)
          val actionTime = baseActionTime +
            ":" + MathStringUtils.fulFullString(String.valueOf(randow.nextInt(59))) +
            ":" + MathStringUtils.fulFullString(String.valueOf(randow.nextInt(59)))
          var searchKeyword: String = null
          var clickCategoryId: Long = null
          var clickProductId: Long = null
          var orderCategoryIds: String = null
          var orderProductIds: String = null
          var payCategoryIds: String = null
          var payProductIds: String = null
          val action = actions(randow.nextInt(4))
          action match {
            case "search" => searchKeyword = keyWord(randow.nextInt(10))
            case "click" => {
              clickCategoryId = String.valueOf(randow.nextInt(100)).asInstanceOf[Long]
              clickProductId = String.valueOf(randow.nextInt(100)).asInstanceOf[Long]
            }
            case "order" => {
              orderCategoryIds = String.valueOf(randow.nextInt(100))
              orderProductIds = String.valueOf(randow.nextInt(100))
            }
            case "pay" => {
              payCategoryIds = String.valueOf(randow.nextInt(100))
              payProductIds = String.valueOf(randow.nextInt(100))
            }
          }
          val row = RowFactory.create(date,
            usrId, sessionId,
            pageId, actionTime,
            searchKeyword, clickCategoryId,
            orderCategoryIds, payCategoryIds,
            payProductIds)
          rows.::(row)
        }
      }
    }
    val rowsRDD = sc.parallelize(rows)
    val schema: StructType = DataTypes.createStructType(util.Arrays.asList(
      DataTypes.createStructField("date", DataTypes.StringType, true),
      DataTypes.createStructField("user_id", DataTypes.LongType, true),
      DataTypes.createStructField("session_id", DataTypes.StringType, true),
      DataTypes.createStructField("page_id", DataTypes.LongType, true),
      DataTypes.createStructField("action_time", DataTypes.StringType, true),
      DataTypes.createStructField("search_keyword", DataTypes.StringType, true),
      DataTypes.createStructField("click_category_id", DataTypes.LongType, true),
      DataTypes.createStructField("click_product_id", DataTypes.LongType, true),
      DataTypes.createStructField("order_category_ids", DataTypes.StringType, true),
      DataTypes.createStructField("order_product_ids", DataTypes.StringType, true),
      DataTypes.createStructField("pay_category_ids", DataTypes.StringType, true),
      DataTypes.createStructField("pay_product_ids", DataTypes.StringType, true)))
    val df: DataFrame = sqlContext.createDataFrame(rowsRDD, schema)
    df.createOrReplaceTempView("user_visit_action")
    val sexes = Array[String]("male", "female")
    for (i <- 0 to 100) {
      val userId = i
      val username = "user" + i
      val name = "name" + i
      val age = randow.nextInt(60)
      val professional = "professional" + randow.nextInt(100)
      val city = "city" + randow.nextInt(100)
      val sex = sexes(randow.nextInt(2))
      val row = RowFactory.create(userId, username, name, age, professional, city, sex)
      rows.::(row)
    }
    val rowRdd = sc.parallelize(rows)
    val schema2 = DataTypes.createStructType(
      util.Arrays.asList(DataTypes.createStructField("user_id", DataTypes.LongType, true),
        DataTypes.createStructField("user_name", DataTypes.StringType, true),
        DataTypes.createStructField("name", DataTypes.StringType, true),
        DataTypes.createStructField("age", DataTypes.IntegerType, true),
        DataTypes.createStructField("professional", DataTypes.StringType, true),
        DataTypes.createStructField("city", DataTypes.StringType, true),
        DataTypes.createStructField("sex", DataTypes.StringType, true)))
    val df2 = sqlContext.createDataFrame(rowsRDD, schema2)
    //user_id,user_name,name,age,professional,city,sex
    df2.createOrReplaceTempView("user_info")
  }
}
