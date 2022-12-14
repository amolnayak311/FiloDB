package filodb.query.exec

import com.esotericsoftware.kryo.io.{Input, Output}
import java.io.{File, FileInputStream, FileOutputStream}
// import java.nio.file.Files
import java.util.UUID
import kamon.Kamon
import kamon.metric.MeasurementUnit
import monix.eval.Task
import monix.reactive.Observable
import scala.collection.mutable
import scala.util.Using

import filodb.core.query._
import filodb.memory.format.{RowReader, ZeroCopyUTF8String => Utf8Str}
import filodb.memory.format.ZeroCopyUTF8String._
import filodb.query._
import filodb.query.Query.qLogger
import filodb.query.exec.binaryOp.BinaryOperatorFunction

import scala.collection.parallel.ParSeq



/**
  * Binary join operator between results of lhs and rhs plan.
  *
  * This ExecPlan accepts two sets of RangeVectors lhs and rhs from child plans.
  * It then does a join of the RangeVectors based on the fields of their keys as
  * dictated by `on` or `ignoring` fields passed as params.
  *
  * Joins can be one-to-one or one-to-many. One-to-One is currently supported using a hash based join.
  *
  * The performance is going to be not-so-optimal since it will involve moving possibly lots of matching range vector
  * data across machines. Histogram based joins can and will be optimized by co-location of bucket, count and sum
  * data and will be joined close to the source using another "HistogramBinaryJoinExec" exec plan.
  *
  * @param lhs ExecPlan that will return results of LHS expression
  * @param rhs ExecPlan that will return results of RHS expression
  * @param binaryOp the binary operator
  * @param cardinality the cardinality of the join relationship as a hint
  * @param on fields from range vector keys to include while performing the join
  * @param ignoring fields from range vector keys to exclude while performing the join
  * @param include labels specified in group_left/group_right to be included from one side
  */
final case class BinaryJoinExec(queryContext: QueryContext,
                                dispatcher: PlanDispatcher,
                                lhs: Seq[ExecPlan],
                                rhs: Seq[ExecPlan],
                                binaryOp: BinaryOperator,
                                cardinality: Cardinality,
                                on: Seq[String],
                                ignoring: Seq[String],
                                include: Seq[String],
                                metricColumn: String,
                                outputRvRange: Option[RvRange],
                                useDiskBasedJoin: Boolean = true) extends NonLeafExecPlan {

  require(cardinality != Cardinality.ManyToMany,
    "Many To Many cardinality is not supported for BinaryJoinExec")
  require(on == Nil || ignoring == Nil, "Cannot specify both 'on' and 'ignoring' clause")
  require(!on.contains(metricColumn), "On cannot contain metric name")

  val onLabels = on.map(Utf8Str(_)).toSet
  val ignoringLabels = ignoring.map(Utf8Str(_)).toSet
  val ignoringLabelsForJoin = ignoringLabels + metricColumn.utf8
  // if onLabels is non-empty, we are doing matching based on on-label, otherwise we are
  // doing matching based on ignoringLabels even if it is empty

  def children: Seq[ExecPlan] = lhs ++ rhs

  protected def args: String = s"binaryOp=$binaryOp, on=$on, ignoring=$ignoring"

  protected def composeStreaming(childResponses: Observable[(Observable[RangeVector], Int)],
                                 schemas: Observable[(ResultSchema, Int)],
                                 querySession: QuerySession): Observable[RangeVector] = ???

  protected[exec] def compose(childResponses: Observable[(QueryResult, Int)],
                              firstSchema: Task[ResultSchema],
                              querySession: QuerySession): Observable[RangeVector] = {
    // TODO: Should be driven based on stats/heuristics?
    if (useDiskBasedJoin) {
      // Uses secondary store to reduce in-memory footprint, The performance might be slower than inMemoryCompose
      memoryEfficientCompose(childResponses, firstSchema, querySession)
    } else
      inMemoryCompose(childResponses, firstSchema, querySession)
  }

  trait IntermediateJoinOperationStore {

    /**
     * On adding one side, the join key for that rv is determined and if a previous RV found, the result
     * will be stitched. Should be stored in a way to make it efficient to retrieve the one side by join key
     *
     * @param rv RangeVector
     * @param responseSeq the response number received in the childResponses of compose
     */
    def addOneSide(rv: SerializedRangeVector, responseSeq: Int): Unit

    /**
     * Gets the oneSide by joinKey
     *
     * @param joinKey the joinKey to get the value by
     * @return the Option of RangeVector based on whether the match was found or not
     */
    def oneSideByJoinKey(joinKey: Map[Utf8Str, Utf8Str]): Option[SerializedRangeVector]

    /**
     * @return Returns the number of one side entries
     */
    def numOneSideEntries: Int

    /**
     * Simply will add rhe response to the many side, unline one side, no attempt to stitch the values will be
     * performed with common join keys
     *
     * @param rv RangeVector
     * @param responseSeq the response number received in the childResponses of compose
     */
    def addManySide(rv: SerializedRangeVector, responseSeq: Int): Unit

    /**
     * Gets the Iterator to the many side
     * @return Iterator to many side
     */
    def manySideIterator: ParSeq[SerializedRangeVector]

    /**
     *
     * @return Returns the number of many side entries
     */
    def numManySideEntries: Int

    /**
     * Saves the key along with oneSide and the other side
     * @param key: the rangevector key of the pair
     * @param oneSide the oneside of the pair
     * @param otherSide otherSide, which can be Many and we may need to stitch them
     */
    def addPair(key: RangeVectorKey, oneSide: SerializedRangeVector, otherSide: SerializedRangeVector): Unit

    /**
     * Gets the number of pairs added to the store
     * @return the count of the number of pairs added
     */
    def numPairs(): Int

    /**
     *
     * @return Iterable of all the pairs added. The Iterable is a three tuple of key, one side and other side of join
     */
    def iteratePairs: Iterator[(RangeVectorKey, RangeVector, RangeVector)]

    /**
     * Cleanup the store and releases resources
     */
    def cleanup(): Unit

    /**
     * From the results added, get the other side by the resKey
     *
     * @param resKey the resultKey used to store the index by
     * @return Option[RangeVector]
     */
    def getOtherByResKey(resKey: RangeVectorKey): Option[RangeVector]

  }

  /**
   * Simple implementation that writes the
   */
  class DiskBasedIntermediateJoinOperationStore(dir: File =
                                                  new File(new File("/tmp/ramdisk/bJoinStore"),
                                                    UUID.randomUUID().toString())
//                                                new File(
//                                                  Files.createTempDirectory("bJoinStore").toFile,
//                                                  UUID.randomUUID().toString())
  )
    extends IntermediateJoinOperationStore {

      dir.mkdirs()
      var oneSideEntries = 0
      var manySideEntries = 0
      var pairs = 0

      private[exec] def joinKeysHash(key: Map[Utf8Str, Utf8Str], algorithm: String = "MD5",
                                     hashAllKeys: Boolean = false): String = {

        // TODO: We can benchmark the performance and see is SHA256 is a steep increase for high cardinality
        //  also can make this configurable
        val digest = java.security.MessageDigest.getInstance(algorithm)
        if (hashAllKeys)
          key.foreach {
            case (k, v)  =>
              digest.update(k.asNewByteArray)
              digest.update(v.asNewByteArray)
          }
        else {
          if (onLabels.nonEmpty) {
            key.foreach {
              case (k, v) if onLabels.contains(k) =>
                digest.update(k.asNewByteArray)
                digest.update(v.asNewByteArray)
              case _ => //NOP
            }
          } else {
            key.foreach {
              case (k, v) if !ignoringLabelsForJoin.contains(k) =>
                digest.update(k.asNewByteArray)
                digest.update(v.asNewByteArray)
              case _ => // NOP
            }
          }
        }


        digest.digest().map("%02x".format(_)).mkString
      }

    override def addOneSide(rv: SerializedRangeVector, responseSeq: Int): Unit = {
      Using.Manager {
        mgr =>
          val jk = joinKeys(rv.key)
          val jkHash = joinKeysHash(jk)
          val oneFile = new File(dir, s"one_$jkHash")
          if (oneFile.exists()) {
            val inStr = mgr(new FileInputStream(oneFile))
            val rvDupe = kryoThreadLocal.get().readObject(mgr(new Input(inStr)), classOf[SerializedRangeVector])
            if (rv.key.labelValues == rvDupe.key.labelValues) {
              val stitched =
                SerializedRangeVector.apply(StitchRvsExec.stitch(rv, rvDupe, outputRvRange), rvDupe.schema.columns)
              writeRvToFile(oneFile, stitched, mgr)
            } else {
              this.cleanup()
              throw new BadQueryException(s"Cardinality $cardinality was used, but many found instead of one for $jk" +
                s"${rvDupe.key.labelValues} and ${rv.key.labelValues} were the violating keys on many side")
            }
          } else {
            writeRvToFile(oneFile, rv, mgr)
            oneSideEntries += 1
          }
      }
    }

    override def oneSideByJoinKey(joinKey: Map[Utf8Str, Utf8Str]): Option[SerializedRangeVector] = {
      val jkHash = joinKeysHash(joinKey)
      val oneFile = new File(dir, s"one_$jkHash")
      if (oneFile.exists()) {
        Using.Manager {
          use =>
            val inStr = use(new FileInputStream(oneFile))
            // TODO: deserialization errors are not handled, they will be thrown back
            val rv = kryoThreadLocal.get().readObject(use(new Input(inStr)), classOf[SerializedRangeVector])
            Some(rv)
        }.get
      } else
        None
    }

    override def numOneSideEntries: Int = oneSideEntries

    private def writeRvToFile(file: File, rv: RangeVector, mgr: Using.Manager): Unit =
      kryoThreadLocal.get().writeObject(mgr(new Output(new FileOutputStream(file))), rv)


    override def addManySide(rv: SerializedRangeVector, responseSeq: Int): Unit =
      Using.Manager {
        mgr =>
          val hash = joinKeysHash(rv.key.labelValues, hashAllKeys = true)
          val manyFile = new File(dir, s"many_$hash")
          writeRvToFile(manyFile, rv, mgr)
          manySideEntries += 1
      }

    override def numManySideEntries: Int = manySideEntries

    override def addPair(key: RangeVectorKey, oneSide: SerializedRangeVector, otherSide: SerializedRangeVector): Unit =
      Using.Manager {
        mgr =>
          val keyHash = joinKeysHash(key.labelValues, hashAllKeys = true)
          val otherFile = new File(dir, s"otherSide_$keyHash")
          if (otherFile.exists()) {
            val other = kryoThreadLocal.get().readObject(
              mgr(new Input(new FileInputStream(otherFile))), classOf[SerializedRangeVector])
            if (otherSide.key.labelValues == other.key.labelValues) {
              val stitchedRv =
                SerializedRangeVector.apply(
                  StitchRvsExec.stitch(otherSide, other, outputRvRange), otherSide.schema.columns)
              writeRvToFile(otherFile, stitchedRv, mgr)
            }
            else
              throw new BadQueryException(s"Non-unique result vectors " +
                s"${other.key.labelValues} and ${otherSide.key.labelValues} " +
                s"found for $key . Use grouping to create unique matching")
          } else {
            writeRvToFile(new File(dir, s"oneSide_$keyHash"), oneSide, mgr)
            writeRvToFile(otherFile, otherSide, mgr)
            val keyFile = new File(dir, s"pairKey_$keyHash")
            kryoThreadLocal.get().writeObject(mgr(new Output(new FileOutputStream(keyFile))), key.labelValues)
            pairs += 1
          }
      }

    override def numPairs(): Int = pairs

    override def iteratePairs: Iterator[(RangeVectorKey, RangeVector, RangeVector)] =
      dir.listFiles((_, name) => name.startsWith("pairKey_"))
        .toIterator.map(keyFile => {
              Using.Manager {
                mgr =>
                  val keyHash = keyFile.toString
                    .substring(keyFile.toString.lastIndexOf(File.separator)).substring("pairKey_".size + 1)
                  val oneRv = kryoThreadLocal.get().readObject(mgr(new Input(
                    new FileInputStream(new File(dir, s"oneSide_$keyHash")))),
                                classOf[SerializedRangeVector])
                  val otherRv = kryoThreadLocal.get().readObject(mgr(new Input(
                    new FileInputStream(new File(dir, s"otherSide_$keyHash")))), classOf[SerializedRangeVector])

                  val keyFileIn = new FileInputStream(keyFile)
                  val keys = kryoThreadLocal.get().readObject(mgr(new Input(keyFileIn)),
                    classOf[Map[Utf8Str, Utf8Str]])
                  (CustomRangeVectorKey(keys), oneRv, otherRv)
              }.get
          }
      )

    override def cleanup(): Unit = {
      // TODO: Implement
      println(s"TODO, Clean up ${dir.toString}")
    }

    override def getOtherByResKey(resKey: RangeVectorKey): Option[RangeVector] = {
      Using.Manager {
        mgr =>
          val keyHash = joinKeysHash(resKey.labelValues)
          val otherSideFile = new File(dir, s"other_$keyHash")
          if (otherSideFile.exists()) {
              Some(
                kryoThreadLocal.get().readObject(mgr(new Input(new FileInputStream(otherSideFile))),
                  classOf[SerializedRangeVector]))
          } else {
            None
          }
      }.get

    }

    override def manySideIterator: ParSeq[SerializedRangeVector] =
      // Increases memory footprint
        dir.listFiles((_, name) => name.startsWith("many_")).toStream.par.map( file =>
          Using.Manager {
            mgr =>
              kryoThreadLocal.get().readObject(
                mgr(new Input(new FileInputStream(file))), classOf[SerializedRangeVector])
          }.get)

  }

//  class InMemoryIntermediateJoinOperationStore extends IntermediateJoinOperationStore {
//
//    val oneSideMap = new mutable.HashMap[Map[Utf8Str, Utf8Str], SerializedRangeVector]()
//    val pairs = new mutable.HashMap[RangeVectorKey, (SerializedRangeVector, SerializedRangeVector)]
//
//    val manySide = new mutable.ArrayBuffer[RangeVector]
//
//    override def numOneSideEntries: Int = oneSideMap.size
//
//    override def addOneSide(rv: SerializedRangeVector, responseSeq: Int): Unit = {
//      val jk = joinKeys(rv.key)
//      // When spread changes, we need to account for multiple Range Vectors with same key coming from different shards
//      // Each of these range vectors would contain data for different time ranges
//      if (oneSideMap.contains(jk)) {
//        val rvDupe = oneSideMap(jk)
//        if (rv.key.labelValues == rvDupe.key.labelValues) {
//          oneSideMap.put(jk,
//            StitchRvsExec.stitch(rv, rvDupe, outputRvRange))
//        } else {
//          this.cleanup()
//          throw new BadQueryException(s"Cardinality $cardinality was used, but many found instead of one for $jk. " +
//            s"${rvDupe.key.labelValues} and ${rv.key.labelValues} were the violating keys on many side")
//        }
//      } else {
//        oneSideMap.put(jk, rv)
//      }
//    }
//
//    override def addManySide(rv: RangeVector, responseSeq: Int): Unit = manySide.append(rv)
//
//    override def numManySideEntries: Int = manySide.size
//
//    def cleanup(): Unit = {
//
//    }
//
//    def getOtherByResKey(resKey: RangeVectorKey): Option[RangeVector] = pairs.get(resKey).map(_._2)
//
//
//    override def oneSideByJoinKey(joinKey: Map[Utf8Str, Utf8Str]): Option[SerializedRangeVector]
//        = oneSideMap.get(joinKey)
//
//    /**
//     * Saves the key along with oneSide and the other side
//     *
//     * @param key       the rangevector key of the pair
//     * @param oneSide   the oneside of the pair
//     * @param otherSide otherSide, which can be Many and we may need to stitch them
//     */
//    override def addPair(key: RangeVectorKey, oneSide: SerializedRangeVector, otherSide: SerializedRangeVector)
//    : Unit =
//      pairs.update(key, pairs.get(key) match {
//        case Some((_, other))  =>
//          if (otherSide.key.labelValues == other.key.labelValues)
//            (oneSide, StitchRvsExec.stitch(otherSide, other, outputRvRange))
//            else
//            throw new BadQueryException(s"Non-unique result vectors " +
//              s"${other.key.labelValues} and ${otherSide.key.labelValues} " +
//              s"found for $key . Use grouping to create unique matching")
//        case None              => (oneSide, otherSide)
//      })
//
//
//    /**
//     * Gets the number of pairs added to the store
//     *
//     * @return the count of the number of pairs added
//     */
//    override def numPairs(): Int = pairs.size
//
//    /**
//     *
//     * @return Iterable of all the pairs added. The Iterable is a three tuple of key, one side and other side of join
//     */
//    override def iteratePairs: Iterable[(RangeVectorKey, RangeVector, RangeVector)] =
//      pairs.iterator.map{ case(key, (one, other)) =>  (key, one, other)}.toIterable
//
//    /**
//     * Gets the Iterator to the many side
//     *
//     * @return Iterator to many side
//     */
//    override def manySideIterator: Iterator[RangeVector] = manySide.iterator
//  }
  //scalastyle:off method.length
  private[exec] def memoryEfficientCompose(childResponses: Observable[(QueryResult, Int)],
                                   firstSchema: Task[ResultSchema],
                                   querySession: QuerySession): Observable[RangeVector] = {

    val numLhsExec = lhs.size
    //val store = new InMemoryIntermediateJoinOperationStore()
    val store = new DiskBasedIntermediateJoinOperationStore()
    var ctr = 0
    childResponses.foldLeft(store) {
              case (store, (qr, i)) =>
                qLogger.debug("==============Starting fold operation===========")
                  if (qr.result.size  > queryContext.plannerParams.joinQueryCardLimit
                    && cardinality == Cardinality.OneToOne)
                    throw new BadQueryException(
                      s"The join in this query has input cardinality of ${qr.result.size} which" +
                      s" is more than limit of ${queryContext.plannerParams.joinQueryCardLimit}." +
                      s" Try applying more filters or reduce time range.")

                // index number in the childResponse is in same order as the Seq[ExecPlan]. Thus, based on cardinality
                // either lhs or rhs is one size of the join operation. We will accordingly add the rangeVectors from
                // the QueryResult determined by te
                if (cardinality == Cardinality.OneToMany) {
                  // LHS is the one side and RHS is the many side
                  // TODO: What about OneToOne?
                  if (i < numLhsExec)
                    qr.result.par.map(_.asInstanceOf[SerializedRangeVector]).foreach(store.addOneSide(_, i))
                  else
                    qr.result.par.map(_.asInstanceOf[SerializedRangeVector]).foreach(store.addManySide(_, i))
                } else {
                  // LHS becomes the many side and RHS is the one side
                  if (i < numLhsExec)
                    qr.result.par.map(_.asInstanceOf[SerializedRangeVector]).foreach(store.addManySide(_, i))
                  else
                    qr.result.par.map(_.asInstanceOf[SerializedRangeVector]).foreach(store.addOneSide(_, i))
                }
                qLogger.debug("==============Ending fold operation===========")
                store

            }.flatMap(store => {
      qLogger.debug("==============Starting iteration on many side===========")
      store.manySideIterator.foreach(
        rvOther => {
          ctr += 1
          val start = System.currentTimeMillis()
          querySession.qContext.checkQueryTimeout(this.getClass.getName)
          val jk = joinKeys(rvOther.key)
          store.oneSideByJoinKey(jk) match {
            case Some(rvOne) =>
              val resKey = resultKeys(rvOne.key, rvOther.key)
              store.addPair(resKey, rvOne, rvOther)
              // OneToOne cardinality case is already handled. this condition handles OneToMany case
              if (store.numPairs() >= queryContext.plannerParams.joinQueryCardLimit)
                throw new BadQueryException(
                  s"The result of this join query has cardinality ${store.numPairs()} and " +
                    s"has reached the limit of ${queryContext.plannerParams.joinQueryCardLimit}." +
                    s" Try applying more filters.")
              val total = System.currentTimeMillis() - start
              qLogger.debug(s"=================Added pair $ctr in $total millis=================")
            case None =>
          }
        })
      qLogger.debug("==============Ending iteration on many side===========")
      Observable.fromIterator(Task.eval {
        store.iteratePairs.map {
          case (key, one, other) =>
            val res = if (cardinality == Cardinality.OneToMany) binOp(one.rows, other.rows)
            else binOp(other.rows, one.rows)
            IteratorBackedRangeVector(key, res, outputRvRange)
        }
      })
    }).guarantee(Task.eval{store.cleanup()})
  }


  private[exec] def inMemoryCompose(childResponses: Observable[(QueryResult, Int)],
                              firstSchema: Task[ResultSchema],
                              querySession: QuerySession): Observable[RangeVector] = {
    val span = Kamon.currentSpan()
    val taskOfResults = childResponses.map {
      case (QueryResult(_, _, result, _, _, _), _)
        if (result.size  > queryContext.plannerParams.joinQueryCardLimit && cardinality == Cardinality.OneToOne) =>
        throw new BadQueryException(s"The join in this query has input cardinality of ${result.size} which" +
          s" is more than limit of ${queryContext.plannerParams.joinQueryCardLimit}." +
          s" Try applying more filters or reduce time range.")
      case (QueryResult(_, _, result, _, _, _), i) => (result, i)
    }.toListL.map { resp =>
      span.mark("binary-join-child-results-available")
      Kamon.histogram("query-execute-time-elapsed-step1-child-results-available",
        MeasurementUnit.time.milliseconds)
        .withTag("plan", getClass.getSimpleName)
        .record(Math.max(0, System.currentTimeMillis - queryContext.submitTime))
      // NOTE: We can't require this any more, as multischema queries may result in not a QueryResult if the
      //       filter returns empty results.  The reason is that the schema will be undefined.
      // require(resp.size == lhs.size + rhs.size, "Did not get sufficient responses for LHS and RHS")
      val lhsRvs = resp.filter(_._2 < lhs.size).flatMap(_._1)
      val rhsRvs = resp.filter(_._2 >= lhs.size).flatMap(_._1)
      // figure out which side is the "one" side
      val (oneSide, otherSide, lhsIsOneSide) =
        if (cardinality == Cardinality.OneToMany) (lhsRvs, rhsRvs, true)
        else (rhsRvs, lhsRvs, false)
      val period = oneSide.headOption.flatMap(_.outputRange)
      // load "one" side keys in a hashmap
      val oneSideMap = new mutable.HashMap[Map[Utf8Str, Utf8Str], RangeVector]()
      oneSide.foreach { rv =>
        val jk = joinKeys(rv.key)
        // When spread changes, we need to account for multiple Range Vectors with same key coming from different shards
        // Each of these range vectors would contain data for different time ranges
        if (oneSideMap.contains(jk)) {
          val rvDupe = oneSideMap(jk)
          if (rv.key.labelValues == rvDupe.key.labelValues) {
            oneSideMap.put(jk, StitchRvsExec.stitch(rv, rvDupe, outputRvRange))
          } else {
            throw new BadQueryException(s"Cardinality $cardinality was used, but many found instead of one for $jk. " +
              s"${rvDupe.key.labelValues} and ${rv.key.labelValues} were the violating keys on many side")
          }
        } else {
          oneSideMap.put(jk, rv)
        }
      }
      // keep a hashset of result range vector keys to help ensure uniqueness of result range vectors
      val results = new mutable.HashMap[RangeVectorKey, ResultVal]()
      case class ResultVal(resultRv: RangeVector, stitchedOtherSide: RangeVector)
      // iterate across the the "other" side which could be one or many and perform the binary operation
      otherSide.foreach { rvOther =>
        val jk = joinKeys(rvOther.key)
        oneSideMap.get(jk).foreach { rvOne =>
          val resKey = resultKeys(rvOne.key, rvOther.key)
          val rvOtherCorrect = if (results.contains(resKey)) {
            val resVal = results(resKey)
            if (resVal.stitchedOtherSide.key.labelValues == rvOther.key.labelValues) {
              StitchRvsExec.stitch(rvOther, resVal.stitchedOtherSide, outputRvRange)
            } else {
              throw new BadQueryException(s"Non-unique result vectors ${resVal.stitchedOtherSide.key.labelValues} " +
                s"and ${rvOther.key.labelValues} found for $resKey . Use grouping to create unique matching")
            }
          } else {
            rvOther
          }

          // OneToOne cardinality case is already handled. this condition handles OneToMany case
          if (results.size >= queryContext.plannerParams.joinQueryCardLimit)
            throw new BadQueryException(s"The result of this join query has cardinality ${results.size} and " +
              s"has reached the limit of ${queryContext.plannerParams.joinQueryCardLimit}. Try applying more filters.")

          val res = if (lhsIsOneSide) binOp(rvOne.rows, rvOtherCorrect.rows) else binOp(rvOtherCorrect.rows, rvOne.rows)
          results.put(resKey, ResultVal(IteratorBackedRangeVector(resKey, res, period), rvOtherCorrect))
        }
      }
      // check for timeout after dealing with metadata, before dealing with numbers
      querySession.qContext.checkQueryTimeout(this.getClass.getName)
      Observable.fromIterable(results.values.map(_.resultRv))
    }
    Observable.fromTask(taskOfResults).flatten
  }
  //scalastyle:on method.length

  private def joinKeys(rvk: RangeVectorKey): Map[Utf8Str, Utf8Str] = {
    if (onLabels.nonEmpty) rvk.labelValues.filter(lv => onLabels.contains(lv._1))
    else rvk.labelValues.filterNot(lv => ignoringLabelsForJoin.contains(lv._1))
  }

  private def resultKeys(oneSideKey: RangeVectorKey, otherSideKey: RangeVectorKey): RangeVectorKey = {
    // start from otherSideKey which could be many or one
    var result = otherSideKey.labelValues
    // drop metric name if math operator
    if (binaryOp.isInstanceOf[MathOperator]) result = result - Utf8Str(metricColumn)

    if (cardinality == Cardinality.OneToOne) {
      result = if (onLabels.nonEmpty) result.filter(lv => onLabels.contains(lv._1)) // retain what is in onLabel list
               else result.filterNot(lv => ignoringLabels.contains(lv._1)) // remove the labels in ignoring label list
    } else if (cardinality == Cardinality.OneToMany || cardinality == Cardinality.ManyToOne) {
      // For group_left/group_right add labels in include from one side. Result should have all keys from many side
      include.foreach { x =>
          val labelVal = oneSideKey.labelValues.get(Utf8Str(x))
          labelVal.foreach { v =>
            if (v.toString.equals(""))
              // If label value is empty do not propagate to result and
              // also delete from result
              result -= Utf8Str(x)
            else
              result += (Utf8Str(x) -> v)
          }
      }
    }
    CustomRangeVectorKey(result)
  }

  private def binOp(lhsRows: RangeVectorCursor,
                    rhsRows: RangeVectorCursor): RangeVectorCursor = {
    new RangeVectorCursor {
      val cur = new TransientRow()
      val binFunc = BinaryOperatorFunction.factoryMethod(binaryOp)
      override def hasNext: Boolean = lhsRows.hasNext && rhsRows.hasNext
      override def next(): RowReader = {
        val lhsRow = lhsRows.next()
        val rhsRow = rhsRows.next()
        cur.setValues(lhsRow.getLong(0), binFunc.calculate(lhsRow.getDouble(1), rhsRow.getDouble(1)))
        cur
      }

      override def close(): Unit = {
        lhsRows.close()
        rhsRows.close()
      }
    }
  }

}

