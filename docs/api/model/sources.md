# Sources types

> Note: {% include types-note.md %}

- `jdbc` - anything that support JDBC connection

```scala
/**
  * Source for anything that support JDBC connection
  * @param sourceId mark to pass to sink
  * @param jdbcUrl example - "jdbc:clickhouse://localhost:8123/default?"
  * @param query SQL query for data
  * @param driverName example - "ru.yandex.clickhouse.ClickHouseDriver"
  * @param datetimeField name of datetime field, could be timestamp and regular time (will be parsed by JodaTime)
  * @param eventsMaxGapMs maximum gap by which source data will be split, i.e. result incidents will be split by these gaps
  * @param defaultEventsGapMs "typical" gap between events, used to unite nearby incidents in one (sessionization)
  * @param partitionFields fields by which data will be split and paralleled physically
  * @param userName for JDBC auth
  * @param password for JDBC auth
  * @param parallelism of source task (not recommended to chagne)
  * @param numParallelSources number of absolutely separate sources to create. Patterns also will be separated by 
  *                           equal (as much as possible) buckets by the max window in pattern (TBD by sum window size) 
  * @param patternsParallelism number of parallel branch nodes splitted after sink stage (node). Patterns also 
  *                            separated by approx. equal buckets by the max window in pattern (TBD by sum window size)
  */
case class JDBCInputConf(
  sourceId: Int,
  jdbcUrl: String,
  query: String,
  driverName: String,
  datetimeField: Symbol,
  eventsMaxGapMs: Long,
  defaultEventsGapMs: Long,
  partitionFields: Seq[Symbol],
  userName: Option[String] = None,
  password: Option[String] = None,
  dataTransformation: Option[SourceDataTransformation] = None,
  parallelism: Option[Int] = None,
  numParallelSources: Option[Int] = Some(1),
  patternsParallelism: Option[Int] = Some(2)
) extends InputConf[Row] { ... }
```

- `influxdb`

```scala
/**
  * Source for InfluxDB
  * @param sourceId simple mark to pass to sink
  * @param dbName
  * @param url to database, for example `http://localhost:8086`
  * @param query Influx SQL query for data
  * @param eventsMaxGapMs maximum gap by which source data will be split, i.e. result incidents will be split by these gaps
  * @param defaultEventsGapMs "typical" gap between events, used to unite nearby incidents in one (sessionization)
  * @param partitionFields fields by which data will be split and paralleled physically
  * @param datetimeField name of datetime field, could be timestamp and regular time (will be parsed by JodaTime)
  * @param userName for auth
  * @param password for auth
  * @param timeoutSec for DB connection
  * @param parallelism of source task (not recommended to chagne)
  * @param numParallelSources number of absolutely separate sources to create. Patterns also will be separated by 
  *                           equal (as much as possible) buckets by the max window in pattern (TBD by sum window size) 
  * @param patternsParallelism number of parallel branch nodes splitted after sink stage (node). Patterns also 
  *                            separated by approx. equal buckets by the max window in pattern (TBD by sum window size)
  */
case class InfluxDBInputConf(
  sourceId: Int,
  dbName: String,
  url: String,
  query: String,
  eventsMaxGapMs: Long,
  defaultEventsGapMs: Long,
  partitionFields: Seq[Symbol],
  datetimeField: Symbol = 'time,
  userName: Option[String] = None,
  password: Option[String] = None,
  timeoutSec: Option[Long] = None,
  dataTransformation: Option[SourceDataTransformation] = None,
  parallelism: Option[Int] = None,
  numParallelSources: Option[Int] = Some(1),
  patternsParallelism: Option[Int] = Some(2)
) extends InputConf[Row] { ... }
```

For supported data transformations, see [Data Transformation](./data-transformation.md).