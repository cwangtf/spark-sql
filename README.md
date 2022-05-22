## 实现 Compact table command
1. 要求：  
添加 compact table 命令，用于合并小文件，例如表 test1 总共有 50000 个文件，每个 1MB，通过该命令，合成为 500 个文件，每个约 100MB。
2. 语法：  
COMPACT TABLE table_identify [partitionSpec] [INTO fileNum FILES]；
3. 说明：
- 基本要求是完成以下功能：COMPACT TABLE test1 INTO 500 FILES；
- 如果添加 partitionSpec，则只合并指定的 partition 目录的文件；
- 如果不加 into fileNum files，则把表中的文件合并成 128MB 大小。
4. 本次作业属于 SparkSQL 的内容，请根据课程内容完成作业。

代码参考：
```
SqlBase.g4
| COMPACT TABLE target=tableIdentifier partitionSpec?
         (INTO fileNum=INTEGER_VALUE FILES)?                           #compactTable
```

### 实现步骤
#### SqlBase.g4
```
statement
| COMPACT TABLE target=tableIdentifier partitionSpec?
     (INTO fileNum=INTEGER_VALUE FILES)?                           #compactTable

ansiNonReserved
| FILES

nonReserved
| FILES

SPARK-KEYWORD-LIST
FILES: 'FILES';
```

#### SparkSqlParser.scala
```scala
override def visitCompactTable(ctx: CompactTableContext): LogicalPlan = withOrigin(ctx) {
  val table: TableIdentifier = visitTableIdentifier(ctx.tableIdentifier())
  val fileNum: Option[Int] = Option(ctx.INTEGER_VALUE().getText.toInt)
  CompactTableCommand(table, fileNum)
}
```

#### CompactTableCommand.scala
```scala
package org.apache.spark.sql.execution.command
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.types.StringType
case class CompactTableCommand(
                                table: TableIdentifier,
                                fileNum: Option[Int]) extends LeafRunnableCommand {
  override def output: Seq[Attribute] = Seq(AttributeReference("no_return", StringType, false)())
  override def run(spark: SparkSession): Seq[Row] = {
    val dataDF: DataFrame = spark.table(table)
    val num: Int = fileNum match {
      case Some(i) => i
      case _ =>
        (spark
          .sessionState
          .executePlan(dataDF.queryExecution.logical)
          .optimizedPlan
          .stats.sizeInBytes / (1024L * 1024L * 128L)
          ).toInt
    }
    log.warn(s"fileNum is $num")
    val tmpTableName = table.identifier + "_tmp"
    dataDF.write.mode(SaveMode.Overwrite).saveAsTable(tmpTableName)
    spark
      .table(tmpTableName)
      .repartition(num)
      .write
      .mode(SaveMode.Overwrite)
      .saveAsTable(table.identifier)
    spark.sql(s"drop table if exists $tmpTableName")
    log.warn("Compacte Table Completed.")
    Seq()
  }
}
```

#### 编译打包
```shell
build/mvn clean package -DskipTests -Phive -Phive-thriftserver
```
##### 执行结果
```shell
[INFO] ------------------------------------------------------------------------
[INFO] Reactor Summary for Spark Project Parent POM 3.2.0:
[INFO]
[INFO] Spark Project Parent POM ........................... SUCCESS [ 19.051 s]
[INFO] Spark Project Tags ................................. SUCCESS [ 27.904 s]
[INFO] Spark Project Sketch ............................... SUCCESS [ 20.963 s]
[INFO] Spark Project Local DB ............................. SUCCESS [  7.341 s]
[INFO] Spark Project Networking ........................... SUCCESS [ 14.421 s]
[INFO] Spark Project Shuffle Streaming Service ............ SUCCESS [ 10.633 s]
[INFO] Spark Project Unsafe ............................... SUCCESS [ 21.567 s]
[INFO] Spark Project Launcher ............................. SUCCESS [  6.862 s]
[INFO] Spark Project Core ................................. SUCCESS [05:08 min]
[INFO] Spark Project ML Local Library ..................... SUCCESS [ 57.383 s]
[INFO] Spark Project GraphX ............................... SUCCESS [01:13 min]
[INFO] Spark Project Streaming ............................ SUCCESS [02:00 min]
[INFO] Spark Project Catalyst ............................. SUCCESS [05:45 min]
[INFO] Spark Project SQL .................................. SUCCESS [08:47 min]
[INFO] Spark Project ML Library ........................... SUCCESS [05:40 min]
[INFO] Spark Project Tools ................................ SUCCESS [ 14.801 s]
[INFO] Spark Project Hive ................................. SUCCESS [03:37 min]
[INFO] Spark Project REPL ................................. SUCCESS [ 48.654 s]
[INFO] Spark Project Hive Thrift Server ................... SUCCESS [02:00 min]
[INFO] Spark Project Assembly ............................. SUCCESS [  8.315 s]
[INFO] Kafka 0.10+ Token Provider for Streaming ........... SUCCESS [ 50.249 s]
[INFO] Spark Integration for Kafka 0.10 ................... SUCCESS [01:16 min]
[INFO] Kafka 0.10+ Source for Structured Streaming ........ SUCCESS [02:22 min]
[INFO] Spark Project Examples ............................. SUCCESS [01:41 min]
[INFO] Spark Integration for Kafka 0.10 Assembly .......... SUCCESS [ 18.023 s]
[INFO] Spark Avro ......................................... SUCCESS [02:00 min]
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time:  47:06 min
[INFO] Finished at: 2022-05-23T00:22:30+08:00
[INFO] ------------------------------------------------------------------------
```

#### 运行spark-sql
```shell
bin/spark-sql
```

#### 新建测试表，插入测试数据，制造小文件
```shell
➜  spark git:(heads/v3.2.0) ✗ bin/spark-sql
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark master: local[*], Application Id: local-1653237194426
spark-sql> CREATE TABLE student (`id` INT, `name` STRING, `age` INT) USING parquet;
Time taken: 6.329 seconds
spark-sql> insert into student values (1, 'wangtf', 18);
Time taken: 5.825 seconds
spark-sql> insert into student values (2, 'wangyc', 6);
Time taken: 0.482 seconds
spark-sql> insert into student values (3, 'wangzl', 33);
Time taken: 0.424 seconds
spark-sql> insert into student values (4, 'wangw', 12);
Time taken: 0.423 seconds
spark-sql> insert into student values (5, 'caimin', 19);
Time taken: 0.381 seconds
spark-sql> insert into student values (6, 'yanj', 56);
Time taken: 0.365 seconds
spark-sql> insert into student values (7, 'test7', 11);
Time taken: 0.473 seconds
spark-sql> insert into student values (8, 'test8', 16);
Time taken: 0.451 seconds
spark-sql> insert into student values (9, 'test9', 99);
Time taken: 0.397 seconds
spark-sql> insert into student values (10, 'shiyin', 86);
Time taken: 0.377 seconds
spark-sql> select * from student;
2	wangyc	6
1	wangtf	18
5	caimin	19
10	shiyin	86
3	wangzl	33
7	test7	11
4	wangw	12
8	test8	16
9	test9	99
6	yanj	56
Time taken: 1.26 seconds, Fetched 10 row(s)
```

#### 查看是否生成小文件
```shell
➜  spark git:(heads/v3.2.0) ✗ ll spark-warehouse/student
total 80
-rw-r--r--  1 wangtf  staff     0B  5 23 00:36 _SUCCESS
-rw-r--r--  1 wangtf  staff   897B  5 23 00:35 part-00000-2e761d1c-cb11-46e6-bcbb-ff75c518facd-c000.snappy.parquet
-rw-r--r--  1 wangtf  staff   904B  5 23 00:34 part-00000-3708d265-a0a5-4a77-9643-4f4ff944cef7-c000.snappy.parquet
-rw-r--r--  1 wangtf  staff   897B  5 23 00:35 part-00000-39b057cd-662a-4f55-bcfc-af22db2bb2cb-c000.snappy.parquet
-rw-r--r--  1 wangtf  staff   890B  5 23 00:35 part-00000-805a24fb-5ad5-4ca1-907d-4c837c72df99-c000.snappy.parquet
-rw-r--r--  1 wangtf  staff   903B  5 23 00:35 part-00000-96f079f4-0686-4343-9d0b-4ddb2bf3764e-c000.snappy.parquet
-rw-r--r--  1 wangtf  staff   903B  5 23 00:36 part-00000-a5838687-5921-45b3-963a-3ac5e6e00f33-c000.snappy.parquet
-rw-r--r--  1 wangtf  staff   897B  5 23 00:35 part-00000-aaf0f597-99e5-4495-af23-773da6f1551b-c000.snappy.parquet
-rw-r--r--  1 wangtf  staff   904B  5 23 00:35 part-00000-ba12ff69-dd06-4907-bd97-cabef804e01f-c000.snappy.parquet
-rw-r--r--  1 wangtf  staff   897B  5 23 00:35 part-00000-fac1825d-b70b-4503-8244-b52862c6a7cc-c000.snappy.parquet
-rw-r--r--  1 wangtf  staff   904B  5 23 00:34 part-00000-fe4f7415-1c69-4671-b7ed-96d48c336100-c000.snappy.parquet
```

#### 运行合并文件命令
```shell
spark-sql> COMPACT TABLE student INTO 2 FILES;
22/05/23 01:01:05 WARN CompactTableCommand: fileNum is 2
22/05/23 01:01:09 WARN CompactTableCommand: Compacte Table Completed.
Time taken: 4.271 seconds
```

#### 查看结果
```shell
➜  spark git:(heads/v3.2.0) ✗ ll spark-warehouse/student
total 16
-rw-r--r--  1 wangtf  staff     0B  5 23 git01:01 _SUCCESS
-rw-r--r--  1 wangtf  staff   987B  5 23 01:01 part-00000-d1613d14-496b-41b4-8dd4-c51227f61e88-c000.snappy.parquet
-rw-r--r--  1 wangtf  staff   938B  5 23 01:01 part-00001-d1613d14-496b-41b4-8dd4-c51227f61e88-c000.snappy.parquet
```