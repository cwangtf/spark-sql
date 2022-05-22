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

#### 运行spark-sql
```shell
bin/spark-sql
```

#### 新建测试表，插入测试数据，制造小文件
```shell
spark-sql> CREATE TABLE student (`id` INT, `name` STRING, `age` INT) USING parquet;
spark-sql> insert into student values (1, 'wangtf', 18);
spark-sql> insert into student values (2, 'wangyc', 6);
spark-sql> insert into student values (3, 'wangzl', 33);
spark-sql> insert into student values (4, 'wangw', 12);
spark-sql> insert into student values (5, 'caimin', 19);
spark-sql> insert into student values (6, 'yanj', 56);
spark-sql> insert into student values (7, 'test7', 11);
spark-sql> insert into student values (8, 'test8', 16);
spark-sql> insert into student values (9, 'test9', 99);
spark-sql> insert into student values (10, 'shiyin', 86);
spark-sql> select * from student;
```

#### 查看是否生成小文件
```shell
spark-warehouse % tree
```

#### 运行合并文件命令
```shell
spark-sql> COMPACT TABLE student INTO 2 FILES;
```

#### 查看结果
```shell

```