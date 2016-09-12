package com.thoughtworks.la.batchlayer

import backtype.cascading.tap.PailTap
import backtype.cascading.tap.PailTap.PailTapOptions
import backtype.hadoop.pail.Pail
import backtype.hadoop.pail.PailSpec
import backtype.hadoop.pail.PailStructure
import cascading.flow.FlowProcess
import cascading.flow.hadoop.HadoopFlowProcess
import cascading.operation.BufferCall
import cascading.operation.FunctionCall
import cascading.tap.Tap
import cascading.tap.hadoop.Hfs
import cascading.scheme.hadoop.SequenceFile
import cascading.tuple.Tuple
import cascading.tuple.TupleEntry
import cascalog.CascalogBuffer
import cascalog.CascalogFunction
import cascalog.ops.IdentityBuffer
import cascalog.ops.RandLong
import com.clearspring.analytics.stream.cardinality.CardinalityMergeException
import com.clearspring.analytics.stream.cardinality.HyperLogLog
import java.io.IOException
import java.net.MalformedURLException
import java.net.URL
import java.util.ArrayList
import java.util.Arrays
import java.util.HashMap
import java.util.Iterator
import java.util.List
import java.util.Map
import java.util.TreeSet
import jcascalog.Api
import jcascalog.Fields
import jcascalog.Option
import jcascalog.Subquery
import jcascalog.op.Count
import jcascalog.op.Sum
import la.schema._
import manning.tap.SplitDataPailStructure
import manning.tap.DataPailStructure
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapred.JobConf
import elephantdb.partition.{ShardingScheme, HashModScheme}
import elephantdb.DomainSpec
import elephantdb.jcascalog.EDB
import java.nio.ByteBuffer
import elephantdb.persistence.JavaBerkDB
import java.io.UnsupportedEncodingException
import manning.test.Data._

import scala.collection.JavaConversions._
/**
  * The entire batch layer for SuperWebAnalytics.com. This is a purely recomputation
  * based implementation. Additional efficiency can be achieved by adding an
  * incremental batch layer as discussed in Chapter 18.
  */
object BatchWorkflow {
  val ROOT = "/tmp/swaroot/"
  val DATA_ROOT = ROOT + "data/"
  val OUTPUTS_ROOT = ROOT + "outputs/"
  val MASTER_ROOT = DATA_ROOT + "master"
  val NEW_ROOT = DATA_ROOT + "new"

  def initTestData() = {
    val fs = FileSystem.get(new Configuration())
    fs.delete(new Path(DATA_ROOT), true)
    fs.delete(new Path(OUTPUTS_ROOT), true)
    fs.mkdirs(new Path(DATA_ROOT))
    fs.mkdirs(new Path(OUTPUTS_ROOT + "edb"))

    val masterPail = Pail.create(MASTER_ROOT, new SplitDataPailStructure())
    val newPail = Pail.create(NEW_ROOT, new DataPailStructure())

    val os = newPail.openWrite()
    os.writeObject(makePageview(1, "http://foo.com/post1", 60))
    os.writeObject(makePageview(3, "http://foo.com/post1", 62))
    os.writeObject(makePageview(1, "http://foo.com/post1", 4000))
    os.writeObject(makePageview(1, "http://foo.com/post2", 4000))
    os.writeObject(makePageview(1, "http://foo.com/post2", 10000))
    os.writeObject(makePageview(5, "http://foo.com/post3", 10600))
    os.writeObject(makeEquiv(1, 3))
    os.writeObject(makeEquiv(3, 5))

    os.writeObject(makePageview(2, "http://foo.com/post1", 60))
    os.writeObject(makePageview(2, "http://foo.com/post3", 62))

    os.close()

  }


  def setApplicationConf() = {
    val conf = new HashMap[Object,Object]()
    var sers = "backtype.hadoop.ThriftSerialization"
    sers ++= ","
    sers ++= "org.apache.hadoop.io.serializer.WritableSerialization"
    conf.put("io.serializations", sers)
    Api.setApplicationConf(conf)
  }

  def attributeTap(path: String, fields: DataUnit._Fields*): PailTap = {
    val opts = new PailTapOptions()

    opts.attrs = Array(fields.map("" ++ _.getThriftFieldId.toString))
    opts.spec = new PailSpec(
      new SplitDataPailStructure().asInstanceOf[PailStructure[_]])

    new PailTap(path, opts)
  }

  def splitDataTap(path: String): PailTap = {
    val opts = new PailTapOptions()
    opts.spec = new PailSpec(
      new SplitDataPailStructure().asInstanceOf[PailStructure[_]])

    new PailTap(path, opts)
  }

  def dataTap(path: String): PailTap = {
    val opts = new PailTapOptions()
    opts.spec = new PailSpec(
      new DataPailStructure().asInstanceOf[PailStructure[_]])

    new PailTap(path, opts)
  }


  def appendNewDataToMasterDataPail(masterPail: Pail[_], snapshotPail: Pail[_]) = {
    val shreddedPail = shred()
    masterPail.absorb(shreddedPail)
  }

  def ingest( masterPail: Pail[_], newDataPail: Pail[_]) = {
    val fs = FileSystem.get(new Configuration())
    fs.delete(new Path("/tmp/swa"), true)
    fs.mkdirs(new Path("/tmp/swa"))

    val snapshotPail = newDataPail.snapshot("/tmp/swa/newDataSnapshot")
    appendNewDataToMasterDataPail(masterPail, snapshotPail)
    newDataPail.deleteSnapshot(snapshotPail)
  }


  def shred(): Pail[_] = {
    val source = dataTap("/tmp/swa/newDataSnapshot")
    val sink = splitDataTap("/tmp/swa/shredded")

    val reduced = new Subquery("?rand", "?data")
      .predicate(source, "_", "?data-in")
      .predicate(new RandLong(), "?rand")
      .predicate(new IdentityBuffer(), "?data-in").out("?data")

    Api.execute(
      sink,
      new Subquery("?data")
        .predicate(reduced, "_", "?data"))
    val shreddedPail = new Pail("/tmp/swa/shredded")
    shreddedPail.consolidate()
    shreddedPail
  }

  class NormalizeURL extends CascalogFunction {
    def operate(process: FlowProcess[_], call: FunctionCall[_]) = {
      val data = (call.getArguments()
        .getObject(0)).asInstanceOf[Data].deepCopy()
      val du = data.get_dataunit()

      if(du.getSetField() == DataUnit._Fields.PAGE_VIEW) {
        normalize(du.get_page_view().get_page())
      } else if(du.getSetField() ==
        DataUnit._Fields.PAGE_PROPERTY) {
        normalize(du.get_page_property().get_id())
      }
      call.getOutputCollector().add(new Tuple(data))
    }

    private def normalize(page: PageID) = {
      if(page.getSetField() == PageID._Fields.URL) {
        val urlStr = page.get_url()
        try {
          val url = new URL(urlStr)
          page.set_url(url.getProtocol() + "://" +
            url.getHost() + url.getPath())
        } catch {
          case e: MalformedURLException => ()
        }
      }
    }

  }

  def normalizeURLs() = {
    val masterDataset = splitDataTap(DATA_ROOT + "master")
    val outTap = splitDataTap("/tmp/swa/normalized_urls")

    Api.execute(outTap,
      new Subquery("?normalized")
        .predicate(masterDataset, "_", "?raw")
        .predicate(new NormalizeURL(), "?raw")
        .out("?normalized"))
  }

  def deduplicatePageviews() = {
    val source = attributeTap(
      "/tmp/swa/normalized_pageview_users",
      DataUnit._Fields.PAGE_VIEW)
    val outTap = splitDataTap("/tmp/swa/unique_pageviews")

    Api.execute(outTap,
      new Subquery("?data")
        .predicate(source, "_", "?data")
        .predicate(Option.DISTINCT, true.asInstanceOf[Object]))
  }

  class ToHourBucket extends CascalogFunction {
    val HOUR_SECS = 60 * 60

    def operate(process: FlowProcess[_], call: FunctionCall[_]) = {
      val timestamp = call.getArguments().getInteger(0)
      val hourBucket = timestamp / HOUR_SECS
      call.getOutputCollector().add(new Tuple(hourBucket.asInstanceOf[Object]))
    }
  }

  class ExtractPageViewFields
      extends CascalogFunction {
    def operate(process: FlowProcess[_], call: FunctionCall[_]) = {
      val data = call.getArguments().getObject(0).asInstanceOf[Data]
      val pageview = data.get_dataunit().get_page_view()
      if(pageview.get_page().getSetField() ==
        PageID._Fields.URL) {
        call.getOutputCollector().add(new Tuple(
          pageview.get_page().get_url().asInstanceOf[Object],
          pageview.get_person().asInstanceOf[Object],
          data.get_pedigree().get_true_as_of_secs().asInstanceOf[Object]
        ))
      }
    }
  }

  class EmitGranularities extends CascalogFunction {
    def operate(process: FlowProcess[_], call: FunctionCall[_]) = {
      val hourBucket = call.getArguments().getInteger(0)
      val dayBucket = hourBucket / 24
      val weekBucket = dayBucket / 7
      val monthBucket = dayBucket / 28

      call.getOutputCollector().add(new Tuple("h", hourBucket.asInstanceOf[Object]))
      call.getOutputCollector().add(new Tuple("d", dayBucket.asInstanceOf[Object]))
      call.getOutputCollector().add(new Tuple("w", weekBucket.asInstanceOf[Object]))
      call.getOutputCollector().add(new Tuple("m", monthBucket.asInstanceOf[Object]))
    }
  }

  class Debug extends CascalogFunction {
    def operate(process: FlowProcess[_], call: FunctionCall[_]) {
      println("DEBUG: " + call.getArguments().toString())
      call.getOutputCollector().add(new Tuple(1.asInstanceOf[Object]))
    }
  }

  def pageviewBatchView(): Subquery = {
    val source = splitDataTap("/tmp/swa/unique_pageviews")

    val hourlyRollup = new Subquery(
      "?url", "?hour-bucket", "?count")
      .predicate(source, "_", "?pageview")
      .predicate(new ExtractPageViewFields(), "?pageview")
      .out("?url", "?person", "?timestamp")
      .predicate(new ToHourBucket(), "?timestamp")
      .out("?hour-bucket")
      .predicate(new Count(), "?count")

    new Subquery(
      "?url", "?granularity", "?bucket", "?total-pageviews")
      .predicate(hourlyRollup, "?url", "?hour-bucket", "?count")
      .predicate(new EmitGranularities(), "?hour-bucket")
      .out("?granularity", "?bucket")
      .predicate(new Sum(), "?count").out("?total-pageviews")
  }

  class ToUrlBucketedKey
      extends CascalogFunction {
    def operate(process: FlowProcess[_], call: FunctionCall[_]) = {
      val url = call.getArguments().getString(0)
      val gran = call.getArguments().getString(1)
      val bucket = call.getArguments().getInteger(2)

      val keyStr = url + "/" + gran + "-" + bucket
      try {
        call.getOutputCollector().add(
          new Tuple(keyStr.getBytes("UTF-8")))
      } catch {
        case e: UnsupportedEncodingException => throw new RuntimeException(e)
      }
    }
  }

  class ToSerializedLong
      extends CascalogFunction {
    def operate(process: FlowProcess[_], call: FunctionCall[_]) = {
      val v = call.getArguments().getLong(0)
      val buffer = ByteBuffer.allocate(8)
      buffer.putLong(v)
      call.getOutputCollector().add(
        new Tuple(buffer.array()))
    }
  }

  def getUrlFromSerializedKey(ser: Array[Byte]): String = {
    try {
      val key = new String(ser, "UTF-8")
      key.substring(0, key.lastIndexOf("/"))
    } catch {
      case e: UnsupportedEncodingException => throw new RuntimeException(e)
    }
  }

  class UrlOnlyScheme extends ShardingScheme {
    def shardIndex(shardKey: Array[Byte], shardCount: Int): Int = {
      val url = getUrlFromSerializedKey(shardKey)
      url.hashCode() % shardCount
    }
  }

  def pageviewElephantDB(pageviewBatchView: Subquery) = {
    val toEdb =
      new Subquery("?key", "?value")
        .predicate(pageviewBatchView,
          "?url", "?granularity", "?bucket", "?total-pageviews")
        .predicate(new ToUrlBucketedKey(),
          "?url", "?granularity", "?bucket")
        .out("?key")
        .predicate(new ToSerializedLong(), "?total-pageviews")
        .out("?value")

    Api.execute(EDB.makeKeyValTap(
      OUTPUTS_ROOT + "edb/pageviews",
      new DomainSpec(new JavaBerkDB(),
        new UrlOnlyScheme(),
        32)),
      toEdb)
  }

  def uniquesElephantDB(uniquesView: Subquery) = {
    val toEdb =
      new Subquery("?key", "?value")
        .predicate(uniquesView,
          "?url", "?granularity", "?bucket", "?value")
        .predicate(new ToUrlBucketedKey(),
          "?url", "?granularity", "?bucket")
        .out("?key")

    Api.execute(EDB.makeKeyValTap(
      OUTPUTS_ROOT + "edb/uniques",
      new DomainSpec(new JavaBerkDB(),
        new UrlOnlyScheme(),
        32)),
      toEdb)
  }

  class ToSerializedString
      extends CascalogFunction {
    def operate(process: FlowProcess[_], call: FunctionCall[_]) {
      val str = call.getArguments().getString(0)

      try {
        call.getOutputCollector().add(
          new Tuple(str.getBytes("UTF-8")))
      } catch {
        case e: UnsupportedEncodingException => throw new RuntimeException(e)
      }
    }
  }

  class ToSerializedLongPair
      extends CascalogFunction {
    def operate(process: FlowProcess[_], call: FunctionCall[_]) = {
      val l1 = call.getArguments().getLong(0)
      val l2 = call.getArguments().getLong(1)
      val buffer = ByteBuffer.allocate(16)
      buffer.putLong(l1)
      buffer.putLong(l2)
      call.getOutputCollector().add(new Tuple(buffer.array()))
    }
  }

  def bounceRateElephantDB(bounceView: Subquery) = {
    val toEdb =
      new Subquery("?key", "?value")
        .predicate(bounceView,
          "?domain", "?bounces", "?total")
        .predicate(new ToSerializedString(),
          "?domain").out("?key")
        .predicate(new ToSerializedLongPair(),
          "?bounces", "?total").out("?value")

    Api.execute(EDB.makeKeyValTap(
      OUTPUTS_ROOT + "edb/bounces",
      new DomainSpec(new JavaBerkDB(),
        new HashModScheme(),
        32)),
      toEdb)
  }

  class ConstructHyperLogLog extends CascalogBuffer {
    def operate(process: FlowProcess[_], call: BufferCall[_]) = {
      val hll = new HyperLogLog(14)
      val it = call.getArgumentsIterator()
      while(it.hasNext()) {
        val tuple = it.next()
        hll.offer(tuple.getObject(0))
      }
      try {
        call.getOutputCollector().add(
          new Tuple(hll.getBytes()))
      } catch {
        case e: IOException => throw new RuntimeException(e)
      }
    }
  }

  class MergeHyperLogLog extends CascalogBuffer {
    def operate(process: FlowProcess[_], call: BufferCall[_]) {
      val it = call.getArgumentsIterator()
      var curr: HyperLogLog = null
      try {
        while(it.hasNext()) {
          val tuple = it.next()
          val serialized = tuple.getObject(0).asInstanceOf[Array[Byte]]
          val hll = HyperLogLog.Builder.build(
            serialized)
          if(curr==null) {
            curr = hll
          } else {
            curr = curr.merge(hll).asInstanceOf[HyperLogLog]
          }
        }
        call.getOutputCollector().add(
          new Tuple(curr.getBytes()))
      } catch{
        case e: IOException => throw new RuntimeException(e)
        case e: CardinalityMergeException => throw new RuntimeException(e)
      }
    }
  }

  def uniquesView(): Subquery = {
    val source = splitDataTap("/tmp/swa/unique_pageviews")

    val hourlyRollup =
      new Subquery("?url", "?hour-bucket", "?hyper-log-log")
        .predicate(source, "_", "?pageview")
        .predicate(
        new ExtractPageViewFields(), "?pageview")
        .out("?url", "?user", "?timestamp")
        .predicate(new ToHourBucket(), "?timestamp")
        .out("?hour-bucket")
        .predicate(new ConstructHyperLogLog(), "?user")
        .out("?hyper-log-log")

    new Subquery(
      "?url", "?granularity", "?bucket", "?aggregate-hll")
      .predicate(hourlyRollup,
        "?url", "?hour-bucket", "?hourly-hll")
      .predicate(new EmitGranularities(), "?hour-bucket")
      .out("?granularity", "?bucket")
      .predicate(new MergeHyperLogLog(), "?hourly-hll")
      .out("?aggregate-hll")
  }

  class ExtractDomain extends CascalogFunction {
    def operate(process: FlowProcess[_], call: FunctionCall[_]) {
      val urlStr = call.getArguments().getString(0)
      try {
        val url = new URL(urlStr)
        call.getOutputCollector().add(new Tuple(url.getAuthority()))
      } catch {
        case e: MalformedURLException => ()
      }
    }
  }

  class AnalyzeVisits extends CascalogBuffer {
    val VISIT_LENGTH_SECS = 60 * 15

    def operate(process: FlowProcess[_], call: BufferCall[_]) = {
      val it = call.getArgumentsIterator()
      var bounces = 0
      var visits = 0
      var lastTime: Integer = null
      var numInCurrVisit = 0
      while(it.hasNext()) {
        val tuple = it.next()
        val timeSecs: Int = tuple.getInteger(0)
        if(lastTime == null ||
          (timeSecs - lastTime) > VISIT_LENGTH_SECS) {
          visits += 1
          if(numInCurrVisit == 1) {
            bounces += 1
          }
          numInCurrVisit = 0
        }
        numInCurrVisit += 1
      }
      if(numInCurrVisit==1) {
        bounces += 1
      }
      call.getOutputCollector().add(new Tuple(visits.asInstanceOf[Object], bounces.asInstanceOf[Object]))
    }
  }

  def bouncesView(): Subquery = {
    val source = splitDataTap("/tmp/swa/unique_pageviews")

    val userVisits =
      new Subquery("?domain", "?user",
        "?num-user-visits", "?num-user-bounces")
        .predicate(source, "_", "?pageview")
        .predicate(
        new ExtractPageViewFields(), "?pageview")
        .out("?url", "?user", "?timestamp")
        .predicate(new ExtractDomain(), "?url")
        .out("?domain")
        .predicate(Option.SORT, "?timestamp")
        .predicate(new AnalyzeVisits(), "?timestamp")
        .out("?num-user-visits", "?num-user-bounces")

    new Subquery("?domain", "?num-visits", "?num-bounces")
      .predicate(userVisits, "?domain", "_",
        "?num-user-visits", "?num-user-bounces")
      .predicate(new Sum(), "?num-user-visits")
      .out("?num-visits")
      .predicate(new Sum(), "?num-user-bounces")
      .out("?num-bounces")
  }

  class EdgifyEquiv extends CascalogFunction {
    def operate(process: FlowProcess[_], call: FunctionCall[_]) = {
      val data = call.getArguments().getObject(0).asInstanceOf[Data]
      val equiv = data.get_dataunit().get_equiv()
      call.getOutputCollector().add(
        new Tuple(equiv.get_id1(), equiv.get_id2()))
    }
  }

  class BidirectionalEdge extends CascalogFunction {
    def operate(process: FlowProcess[_], call: FunctionCall[_]) = {
      val node1 = call.getArguments().getObject(0)
      val node2 = call.getArguments().getObject(1)
      if(!node1.equals(node2)) {
        call.getOutputCollector().add(
          new Tuple(node1, node2))
        call.getOutputCollector().add(
          new Tuple(node2, node1))
      }
    }
  }

  class IterateEdges extends CascalogBuffer {
    def operate(process: FlowProcess[_], call: BufferCall[_]) = {
      val grouped = call.getGroup().getObject(0).asInstanceOf[PersonID]
      val allIds = new TreeSet[PersonID]()
      allIds.add(grouped)

      val it = call.getArgumentsIterator()
      while(it.hasNext()) {
        allIds.add(it.next().getObject(0).asInstanceOf[PersonID])
      }

      val allIdsIt = allIds.iterator()
      val smallest = allIdsIt.next()
      val isProgress = allIds.size() > 2 && !grouped.equals(smallest)
      while(allIdsIt.hasNext()) {
        val id = allIdsIt.next()
        call.getOutputCollector().add(
          new Tuple(smallest, id, isProgress.asInstanceOf[Object]))
      }
    }
  }

  class MakeNormalizedPageview
      extends CascalogFunction {
    def operate(process: FlowProcess[_], call: FunctionCall[_]) = {
      val newId = call.getArguments().getObject(0).asInstanceOf[PersonID]
      val data = call.getArguments().getObject(1).asInstanceOf[Data].deepCopy()
      if(newId!=null) {
        data.get_dataunit().get_page_view().set_person(newId)
      }
      call.getOutputCollector().add(new Tuple(data))
    }
  }

  def runUserIdNormalizationIteration(i: Int) = {
    val source = Api.hfsSeqfile(
      "/tmp/swa/equivs" + (i - 1))
    val sink = Api.hfsSeqfile("/tmp/swa/equivs" + i)

    var iteration: Object = new Subquery(
      "?b1", "?node1", "?node2", "?is-new")
      .predicate(source, "?n1", "?n2")
      .predicate(new BidirectionalEdge(), "?n1", "?n2")
      .out("?b1", "?b2")
      .predicate(new IterateEdges(), "?b2")
      .out("?node1", "?node2", "?is-new")

    iteration = Api.selectFields(iteration,
      new Fields("?node1", "?node2", "?is-new"))

    val newEdgeSet = new Subquery("?node1", "?node2")
      .predicate(iteration, "?node1", "?node2", "?is-new")
      .predicate(Option.DISTINCT, true.asInstanceOf[Object])

    val progressEdgesSink = new Hfs(new SequenceFile(cascading.tuple.Fields.ALL), "/tmp/swa/equivs" + i + "-new")
    val progressEdges: Subquery = new Subquery("?node1", "?node2")
      .predicate(iteration, "?node1", "?node2", true.asInstanceOf[Object])

    Api.execute(Arrays.asList(sink.asInstanceOf[Object], progressEdgesSink),
      Arrays.asList(newEdgeSet.asInstanceOf[Object], progressEdges))

    progressEdgesSink
  }

  def normalizeUserIds() = {
    val equivs = attributeTap("/tmp/swa/normalized_urls",
      DataUnit._Fields.EQUIV)
    Api.execute(Api.hfsSeqfile("/tmp/swa/equivs0"),
      new Subquery("?node1", "?node2")
        .predicate(equivs, "_", "?data")
        .predicate(new EdgifyEquiv(), "?data")
        .out("?node1", "?node2"))
    var i = 1
    var hasMore = true
    while(hasMore) {
      val progressEdgesSink = runUserIdNormalizationIteration(i)

      hasMore = new HadoopFlowProcess(new JobConf())
        .openTapForRead(progressEdgesSink.asInstanceOf[Tap[_, _, _]])
        .hasNext()
      if (hasMore) i += 1
    }

    val pageviews = attributeTap("/tmp/swa/normalized_urls",
      DataUnit._Fields.PAGE_VIEW)
    val newIds = Api.hfsSeqfile("/tmp/swa/equivs" + i)
    val result = splitDataTap(
      "/tmp/swa/normalized_pageview_users")

    Api.execute(result,
      new Subquery("?normalized-pageview")
        .predicate(newIds, "!!newId", "?person")
        .predicate(pageviews, "_", "?data")
        .predicate(new ExtractPageViewFields(), "?data")
        .out("?userid", "?person", "?timestamp")
        .predicate(new MakeNormalizedPageview(),
          "!!newId", "?data").out("?normalized-pageview"))
  }

  def batchWorkflow() = {
    setApplicationConf()

    val masterPail = new Pail(MASTER_ROOT)
    val newDataPail = new Pail(NEW_ROOT)

    ingest(masterPail, newDataPail)
    normalizeURLs()
    normalizeUserIds()
    deduplicatePageviews()
    pageviewElephantDB(pageviewBatchView())
    uniquesElephantDB(uniquesView())
    bounceRateElephantDB(bouncesView())
  }
}
