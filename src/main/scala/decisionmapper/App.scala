package decisionmapper

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.json4s.DefaultFormats

object App {

  val spark: SparkSession = SparkSession.builder().config(
    new SparkConf().setMaster("local[*]")
  ).getOrCreate()

  /**
    * Filters the dataset, applies rules provided in args and then
    * counts statistics by resulting columns
    *
    * args(0) - the path to input csv
    * args(1) - the path to columns settings
    */
  def main(args: Array[String]): Unit = {
    val Array(dataPath, rulesPath) = args

    val dataFrame = spark.read.format("csv").option("header", "true").load(dataPath)
    val filtered = filterDF(dataFrame)
    filtered.show
    val rules = readRules(rulesPath)
    val dfRemovedCols = removeColumns(filtered, rules.map(_.existing_col_name))
    val resultDF = renameAndCastColumns(dfRemovedCols, rules)
    println(resultDF.schema)
    resultDF.show

    import org.json4s.jackson.Serialization.writePretty
    implicit val _ = DefaultFormats
    println(writePretty(collectStat(resultDF)))
  }

  def filterDF(dataFrame: DataFrame): Dataset[Row] = {
    dataFrame.filter { row =>
      (0 until row.schema.length).forall(colIndex => {
        val str = row.getString(colIndex)
        str == null || !str.trim().isEmpty
      })
    }
  }

  def readRules(rulesPath: String): Array[Rule] = {
    import org.json4s._
    import org.json4s.jackson.JsonMethods._

    import scala.io.Source._
    implicit val _ = DefaultFormats

    parse(fromFile(rulesPath).getLines().mkString).extract[Array[Rule]]
  }

  def removeColumns(df: DataFrame, colsInRules: Array[String]): DataFrame = {
    val colsNotInRules = df.columns.diff(colsInRules)
    df.drop(colsNotInRules: _*)
  }

  def renameAndCastColumns(df: DataFrame, rules: Array[Rule]): DataFrame = {
    rules.foldLeft(df) { (df, rule) =>
      import org.apache.spark.sql.functions.to_date

      val result = rule.new_data_type match {
        case "date" =>
          df.withColumn(
            rule.existing_col_name,
            to_date(df(rule.existing_col_name), rule.date_expression.orNull)
          )
        case _ =>
          df.withColumn(
            rule.existing_col_name,
            df(rule.existing_col_name)
              .cast(rule.new_data_type)
          )
      }
      result.withColumnRenamed(rule.existing_col_name, rule.new_col_name)

    }
  }


  def collectStat(df: DataFrame): Array[Stat] = {
    def statForColumn(columnName: String): Stat = {
      import spark.implicits._

      val filtered = df.select(columnName).filter(!_.anyNull)
      filtered.cache()
      Stat(columnName,
        filtered.distinct().count(),
        filtered.groupBy(columnName)
          .count()
          .map(row => row.get(0).toString -> row.get(1).toString)
          .collect()
      )
    }

    df.columns.map(statForColumn)
  }
}

case class Rule(existing_col_name: String, new_col_name: String, new_data_type: String, date_expression: Option[String])

case class Stat(Column: String, Unique_values: Long, Values: Array[(String, String)])