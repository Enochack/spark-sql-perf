package com.databricks.spark.sql.perf

import com.databricks.spark.sql.perf.tpcds.TPCDSTables
import com.databricks.spark.sql.perf.tpch.TPCHTables
import org.apache.spark.sql.SparkSession

case class GenDataConfig(
  // Spark master url
  master: String = "local[*]",
  // TPCDS or TPCH tables
  tables: String = "tpcds",
  // root directory of location to create data in.
  location: String = "/user/spark/warehouse",
  // name of database to create.
  database: String = "tpcds1g",
  // scaleFactor defines the size of the dataset to generate (in GB).
  scaleFactor: String = "1",
  // true to replace DecimalType with DoubleType
  useDoubleForDecimal: Boolean = false,
  // true to replace DateType with StringType
  useStringForDate: Boolean = false,
  // valid spark format like parquet "parquet".
  format: String = "parquet",
  // location of dsdgen
  genDir: String = "/tmp/tpcds-kit/tools",
  // overwrite the data that is already there
  overwrite: Boolean = true,
  // create the partitioned fact tables
  partitionTables: Boolean = false,
  // shuffle to get partitions coalesced into single files.
  clusterByPartitionColumns: Boolean = false,
  // true to filter out the partition with NULL key value
  filterOutNullPartitionValues: Boolean = false,
  // "" means generate all tables
  tableFilter: String = "",
  // how many dsdgen partitions to run - number of input tasks.
  numPartitions: Int = 2)

/**
 * Generate a new TPCDS dataset.
 */
object GenData {

  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[GenDataConfig]("spark-sql-perf") {
      head("spark-sql-perf", "0.5.1-SNAPSHOT")
      opt[String]("master")
        .action { (x, c) => c.copy(master = x) }
        .text("the Spark master to use, default to local[*]")
      opt[String]("tables")
        .action { (x, c) => c.copy(master = x) }
        .text("tpcds or tpch tables to create, default to tpcds")
      opt[String]("location")
        .action { (x, c) => c.copy(location = x) }
        .text("root directory of location to create data in")
        .required()
      opt[String]("database")
        .action { (x, c) => c.copy(database = x) }
        .text("name of database to create")
        .required()
      opt[String]("scale-factor")
        .action((x, c) => c.copy(scaleFactor = x))
        .text("the size of the dataset to generate (in GB)")
        .required()
      opt[Boolean]("use-double-for-decimal")
        .action((x, c) => c.copy(useDoubleForDecimal = x))
        .text("true to replace DecimalType with DoubleType")
      opt[Boolean]("use-string-for-date")
        .action((x, c) => c.copy(useStringForDate = x))
        .text("true to replace DateType with StringType")
      opt[String]("format")
        .action((x, c) => c.copy(format = x))
        .text("valid spark format like parquet")
      opt[String]("gen-dir")
        .action((x, c) => c.copy(genDir = x))
        .text("location of dsdgen / dbgen")
        .required()
      opt[Boolean]("overwrite")
        .action((x, c) => c.copy(overwrite = x))
        .text("overwrite the data that is already there")
      opt[Boolean]("partition-tables")
        .action((x, c) => c.copy(partitionTables = x))
        .text("true to create the partitioned fact tables")
      opt[Boolean]("cluster-by-partiton-columns")
        .action((x, c) => c.copy(clusterByPartitionColumns = x))
        .text("true to shuffle to get partitions coalesced into single files")
      opt[Boolean]("filter-out-null-partition-values")
        .action((x, c) => c.copy(filterOutNullPartitionValues = x))
        .text("true to filter out the partition with NULL key value")
      opt[String]("table-filter")
        .action((x, c) => c.copy(tableFilter = x))
        .text("comma separated table names and \"\" means generate all tables")
      opt[Int]("num-partitions")
        .action((x, c) => c.copy(numPartitions = x))
        .text("how many dsdgen partitions to run - number of input tasks")
        .required()
      help("help")
        .text("prints this usage text")
    }

    parser.parse(args, GenDataConfig()) match {
      case Some(conf) => run(conf)
      case None => sys.exit(1)
    }
  }

  private def run(conf: GenDataConfig): Unit = {
    lazy val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master(conf.master)
      .enableHiveSupport()
      .getOrCreate()

    lazy val sql = spark.sql _

    val tables = conf.tables match {
      case "tpcds" =>
        new TPCDSTables(spark.sqlContext,
          dsdgenDir = conf.genDir,
          scaleFactor = conf.scaleFactor,
          useDoubleForDecimal = conf.useDoubleForDecimal,
          useStringForDate = conf.useStringForDate)
      case "tpch" =>
        new TPCHTables(spark.sqlContext,
          dbgenDir = conf.genDir,
          scaleFactor = conf.scaleFactor,
          useDoubleForDecimal = conf.useDoubleForDecimal,
          useStringForDate = conf.useStringForDate)
      case invalid =>
        throw new IllegalArgumentException(s"Unsupported type of tables: $invalid, supported" +
          s"types: [tpcds, tpch]")
    }


    tables.genData(
      location = conf.location,
      format = conf.format,
      overwrite = conf.overwrite,
      partitionTables = conf.partitionTables,
      clusterByPartitionColumns = conf.clusterByPartitionColumns,
      filterOutNullPartitionValues = conf.filterOutNullPartitionValues,
      tableFilter = conf.tableFilter,
      numPartitions = conf.numPartitions)

    // Create the specified database
    sql(s"create database ${conf.database}")
    // Create metastore tables in a specified database for your data.
    // Once tables are created, the current database will be switched to the specified database.
    tables.createExternalTables(
      conf.location,
      conf.format,
      conf.database,
      overwrite = conf.overwrite,
      discoverPartitions = conf.partitionTables)
  }
}
