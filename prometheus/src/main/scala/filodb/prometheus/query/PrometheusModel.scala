package filodb.prometheus.query

import remote.RemoteStorage._

import filodb.core.GlobalConfig
import filodb.core.binaryrecord2.{BinaryRecordRowReader, StringifyMapItemConsumer}
import filodb.core.metadata.Column.ColumnType
import filodb.core.metadata.PartitionSchema
import filodb.core.query.{Result => _, _}
import filodb.prometheus.parse.Parser.REGEX_MAX_LEN
import filodb.query.{QueryResult => FiloQueryResult, _}
import filodb.query.AggregationOperator.Avg
import filodb.query.exec.{ExecPlan, HistToPromSeriesMapper}

object PrometheusModel {
  import com.softwaremill.quicklens._
  val conf = GlobalConfig.defaultsFromUrl
  val queryConfig = conf.getConfig("filodb.query")

  /**
   * If the result contains Histograms, automatically convert them to Prometheus vector-per-bucket output
   */
  def convertHistToPromResult(qr: FiloQueryResult, sch: PartitionSchema): FiloQueryResult = {
    if (!qr.resultSchema.isEmpty && qr.resultSchema.columns(1).colType == ColumnType.HistogramColumn) {
      val mapper = HistToPromSeriesMapper(sch)
      val promVectors = qr.result.flatMap(mapper.expandVector)
      qr.copy(result = promVectors)
        .modify(_.resultSchema.columns).using(_.updated(1, ColumnInfo("value", ColumnType.DoubleColumn)))
    } else qr
  }

  /**
    * Converts a prometheus read request to a Seq[LogicalPlan]
    */
  def toFiloDBLogicalPlans(readRequest: ReadRequest): Seq[LogicalPlan] = {
    for { i <- 0 until readRequest.getQueriesCount } yield {
      val q = readRequest.getQueries(i)
      val interval = IntervalSelector(q.getStartTimestampMs, q.getEndTimestampMs)
      val filters = for { j <- 0 until q.getMatchersCount } yield {
        val m = q.getMatchers(j)
        val filter = m.getType match {
          case MatchType.EQUAL => Filter.Equals(m.getValue)
          case MatchType.NOT_EQUAL => Filter.NotEquals(m.getValue)
          case MatchType.REGEX_MATCH =>
                            require(m.getValue.length <= REGEX_MAX_LEN, s"Regular expression filters should " +
                              s"be <= ${REGEX_MAX_LEN} characters")
                            Filter.EqualsRegex(m.getValue)
          case MatchType.REGEX_NO_MATCH =>
                            require(m.getValue.length <= REGEX_MAX_LEN, s"Regular expression filters should " +
                              s"be <= ${REGEX_MAX_LEN} characters")
                            Filter.NotEqualsRegex(m.getValue)
        }
        ColumnFilter(m.getName, filter)
      }
      RawSeries(interval, filters, Nil, None, None)
    }
  }

  def toPromReadResponse(qrs: Seq[FiloQueryResult]): Array[Byte] = {
    val b = ReadResponse.newBuilder()
    qrs.foreach(r => b.addResults(toPromQueryResult(r)))
    b.build().toByteArray()
  }

  // Creates Prometheus protobuf QueryResult output
  def toPromQueryResult(qr: FiloQueryResult): QueryResult = {
    val b = QueryResult.newBuilder()
    qr.result.foreach{ srv =>
      b.addTimeseries(toPromTimeSeries(srv))
    }
    b.build()
  }

  /**
    * Used to send out raw data as Prometheus TimeSeries protobuf
    */
  def toPromTimeSeries(srv: RangeVector): TimeSeries = {
    val b = TimeSeries.newBuilder()
    srv.key.labelValues.foreach {lv =>
      b.addLabels(LabelPair.newBuilder().setName(lv._1.toString).setValue(lv._2.toString))
    }
    srv.rows.foreach { row =>
      // no need to remove NaN here.
      b.addSamples(Sample.newBuilder().setTimestampMs(row.getLong(0)).setValue(row.getDouble(1)))
    }
    b.build()
  }

  def toPromSuccessResponse(qr: FiloQueryResult, verbose: Boolean): SuccessResponse = {
    val results = if (qr.resultSchema.columns.nonEmpty && qr.resultSchema.columns.length > 1 &&
                      qr.resultSchema.columns(1).colType == ColumnType.HistogramColumn)
                    qr.result.map(toHistResult(_, verbose, qr.resultType))
                  else
                    qr.result.map(toPromResult(_, verbose, qr.resultType))
    SuccessResponse(
      Data(toPromResultType(qr.resultType), results.filter(r => r.values.nonEmpty || r.value.isDefined)),
      "success",
      Some(qr.mayBePartial), qr.partialResultReason,
      Some(toQueryStatistics(qr.queryStats)),
      Some(toQueryWarningsResponse(qr.warnings))
    )
  }

  def toPromExplainPlanResponse(ex: ExecPlan): ExplainPlanResponse = {
    ExplainPlanResponse(ex.getPlan())
  }

  def toPromResultType(r: QueryResultType): String = {
    r match {
      case QueryResultType.RangeVectors => "matrix"
      case QueryResultType.InstantVector => "vector"
      case QueryResultType.Scalar => "scalar"
    }
  }

  def toLabelValuesResponse(qr: FiloQueryResult, verbose: Boolean, typ: QueryResultType,
                         mayBePartial: Option[Boolean]): MetadataSuccessResponse = {
    val values = qr.result.flatMap(srv => srv.rows.map(row => {
      val br = row.asInstanceOf[BinaryRecordRowReader]
      LabelSampl(br.schema.colValues(br.recordBase, br.recordOffset, br.schema.colNames).head)
    }))
    MetadataSuccessResponse(values, "success", mayBePartial)
  }

  def toMetadataMapResponse(qr: FiloQueryResult, verbose: Boolean, typ: QueryResultType,
                         mayBePartial: Option[Boolean]): MetadataSuccessResponse = {
    val values = qr.result.flatMap(srv => srv.rows.map(row => {
      val br = row.asInstanceOf[BinaryRecordRowReader]
      val consumer = new StringifyMapItemConsumer
      br.schema.consumeMapItems(br.recordBase, br.recordOffset, 0, consumer)
      MetadataMapSampl(consumer.stringPairs.toMap)
    }))
    MetadataSuccessResponse(values, "success", mayBePartial)
  }

  /**
    * Used to send out HTTP response for double-based data
    */
  def toPromResult(srv: RangeVector, verbose: Boolean, typ: QueryResultType): Result = {
    val tags = srv.key.labelValues.map { case (k, v) => (k.toString, v.toString)} ++
                (if (verbose) makeVerboseLabels(srv.key)
                else Map.empty)
    val samples = srv.rows.filter(!_.getDouble(1).isNaN).map { r =>
      Sampl(r.getLong(0) / 1000, r.getDouble(1))
    }.toSeq

    typ match {
      case QueryResultType.RangeVectors =>
        Result(tags,
          // remove NaN in HTTP results
          // Known Issue: Until we support NA in our vectors, we may not be able to return NaN as an end-of-time-series
          // in HTTP raw query results.
          if (samples.isEmpty) None else Some(samples),
          None
        )
      case QueryResultType.InstantVector =>
        Result(tags, None, samples.headOption)
      case QueryResultType.Scalar =>
        Result(tags, None, samples.headOption)
    }
  }

  def toHistResult(srv: RangeVector,
                   verbose: Boolean,
                   typ: QueryResultType,
                   processMultiPartition: Boolean = true): Result = {
    val tags = srv.key.labelValues.map { case (k, v) => (k.toString, v.toString)} ++
                (if (verbose) makeVerboseLabels(srv.key)
                else Map.empty)
    val samples = srv.rows.map { r => (r.getLong(0), r.getHistogram(1)) }.collect {
      // Don't remove empty histogram for remote query as it is needed for stitching with local results
      case (t, h) if (h.numBuckets > 0 || !processMultiPartition) =>
        val buckets = (0 until h.numBuckets).map { b =>
          val le = h.bucketTop(b)
          (if (le == Double.PositiveInfinity) "+Inf" else le.toString) -> h.bucketValue(b)
        }
        HistSampl(t / 1000, buckets.toMap)
    }.toSeq

    typ match {
      case QueryResultType.RangeVectors =>
        Result(tags, if (samples.isEmpty) None else Some(samples), None)
      case QueryResultType.InstantVector =>
        Result(tags, None, samples.headOption)
      case QueryResultType.Scalar => ???
    }
  }

  def toAvgResult(srv: RangeVector,
                   verbose: Boolean,
                   typ: QueryResultType,
                   processMultiPartition: Boolean = true): Result = {
    val tags = srv.key.labelValues.map { case (k, v) => (k.toString, v.toString)} ++
      (if (verbose) makeVerboseLabels(srv.key)
      else Map.empty)
    val samples = srv.rows.map { r => AvgSampl(r.getLong(0)/1000, r.getDouble(1),
      r.getLong(2))
    }.toSeq

    Result(tags, None, None, Some(AggregateResponse(Avg.entryName, samples)))
  }

  def toStdValResult(srv: RangeVector,
                  verbose: Boolean,
                  typ: QueryResultType,
                  processMultiPartition: Boolean = true): Result = {
    val tags = srv.key.labelValues.map { case (k, v) => (k.toString, v.toString)} ++
      (if (verbose) makeVerboseLabels(srv.key)
      else Map.empty)
    val samples = srv.rows.map { r => StdValSampl(r.getLong(0)/1000, r.getDouble(1),
      r.getDouble(2), r.getLong(3))
    }.toSeq

    Result(tags, None, None, Some(AggregateResponse(QueryFunctionConstants.stdVal, samples)))
  }

  def makeVerboseLabels(rvk: RangeVectorKey): Map[String, String] = {
    Map("_shards_" -> rvk.sourceShards.mkString(","),
      "_partIds_" -> rvk.partIds.mkString(","),
      "_type_" -> rvk.schemaNames.mkString(","))
  }

  def toPromErrorResponse(qe: filodb.query.QueryError): ErrorResponse = {
    ErrorResponse(qe.t.getClass.getSimpleName, qe.t.getMessage, "error", Some(toQueryStatistics(qe.queryStats)))
  }

  def toQueryStatistics(qs: QueryStats): Seq[QueryStatistics] = qs.stat.map(stat =>
    QueryStatistics(stat._1, stat._2.timeSeriesScanned.get(),
      stat._2.dataBytesScanned.get(), stat._2.resultBytes.get(), stat._2.cpuNanos.get())
  ).toSeq

  def toQueryWarningsResponse(qw: QueryWarnings): QueryWarningsResponse = {
    QueryWarningsResponse(
      execPlanSamples = qw.execPlanSamples.get(),
      execPlanResultBytes = qw.execPlanResultBytes.get(),
      groupByCardinality = qw.groupByCardinality.get(),
      joinQueryCardinality = qw.joinQueryCardinality.get(),
      timeSeriesSamplesScannedBytes = qw.timeSeriesSamplesScannedBytes.get(),
      timeSeriesScanned = qw.timeSeriesScanned.get(),
      rawScannedBytes = qw.rawScannedBytes.get()
    )
  }

}