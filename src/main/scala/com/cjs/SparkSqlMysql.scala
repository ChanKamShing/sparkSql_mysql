package com.cjs

import org.apache.calcite.avatica.ColumnMetaData.StructType
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession, types}

import scala.collection.mutable.ArrayBuffer

object SparkSqlMysql {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

        val conf = new SparkConf()
            .set("spark.sql.warehouse.dir","file:///e:/tmp/spark-warehouse")
            .set("spark.some.config.option","some-value")

        val ss = SparkSession
            .builder()
            .config(conf)
            .appName("test_DF_API")
            .master("local[2]") //单机版配置，集群情况下，可以去掉
            .getOrCreate()

        val ip = "127.0.0.1"    //本地ip，如果是集群，则要修改为装有mysql的机器ip
        val port = "3306"
        val databaseName = "bigdatas"
        val username = "root"
        val password = "123456"
        val url = s"jdbc:mysql://$ip:$port/$databaseName"

        val stuInfoTable = "student_info"   //学生表的表名
        val scoreTable = "score"    //成绩表的表名

        //连接mysql数据库
        val mysqlDFReader = ss.read.format("jdbc")
            .option("url",url)
            .option("driver","com.mysql.jdbc.Driver")
            .option("user",username)
            .option("password",password)

        val stuInfoDF = mysqlDFReader.option("dbtable",stuInfoTable).load()
//        stuInfoDF.show()
        //        val stuInfoDFNew = setStuInfoStructType(ss,stuInfoDF)
        //        stuInfoDFNew.show()

        val scoreDF = mysqlDFReader.option("dbtable",scoreTable).load()
//        scoreDF.show()
//        val scoreDFNew = setScoreStructType(ss,scoreDF)
//        scoreDFNew.show()
    }

    def setStuInfoStructType(ss: SparkSession, dataFrame:DataFrame)={
        import ss.implicits._
        val rdd = dataFrame.rdd.map(x=>
            Row(x.getInt(0),x.getString(1),x.getString(2),x.getDate(3))
        )
        ss.createDataFrame(rdd,getStuInfoStructType())
    }

    //自定义stuInfoDF的结构
    def getStuInfoStructType()={
        val stuInfoStructFields = new ArrayBuffer[StructField]()
        stuInfoStructFields += StructField("id",DataTypes.IntegerType,nullable = true)
        stuInfoStructFields += StructField("name",DataTypes.StringType,nullable = true)
        stuInfoStructFields += StructField("gender",DataTypes.StringType,nullable = true)
        stuInfoStructFields += StructField("birthday",DataTypes.DateType,nullable = true)
        types.StructType(stuInfoStructFields)
    }

    def getScoreStructType() = {
        val fields = new ArrayBuffer[StructField]()
        fields += StructField("id",DataTypes.IntegerType,nullable = true)
        fields += StructField("name",DataTypes.StringType,nullable = true)
        fields += StructField("score",DataTypes.FloatType,nullable = true)
        types.StructType(fields)
    }

    def setScoreStructType(ss:SparkSession, dataFrame:DataFrame)={
        import ss.implicits._
        val rdd = dataFrame.rdd.map(x=>
            Row(x.getInt(0),x.getString(1),x.getFloat(2))
        )
        ss.createDataFrame(rdd,getScoreStructType())
    }

    /**
      * 随机分组
      * 在数据挖掘中，将数据分成两组，一组用于训练，一组用于测试
      * @param dataDF
      */
    def randomSplit(dataDF:DataFrame)={
        val weight = Array[Double](0.7,0.3)

        val splited = dataDF.randomSplit(weight)
        splited(0).show()
        splited(1).show()
    }
}

/**
  * 集群提交命令：
  * cd $SPARK_HOME
  * ./bin/spark-submit \
  *     --class com.cjs.SparkSqlMysql \
  *     --master yarn-cluster \
  *     --executor-memory 1G \
  *     --total-executor-cores 2 \
  *     --jars $HIVE_HOME/lib/mysql-connector-java-5.1.47-bin.jar,$SPARK_HOME/jars/datanucleus-api-jdo-3.2.6.jar,$SPARK_HOME/jars/datanucleus-core-3.2.10.jar,$SPARK_HOME/jars/datanucleus-rdbms-3.2.9.jar,$SPARK_HOME/jars/guava-14.0.1.jar \
  */
