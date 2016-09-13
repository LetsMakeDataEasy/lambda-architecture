package la.speedlayer

import backtype.storm.Config
import backtype.storm.LocalCluster
import backtype.storm.tuple.Fields
import backtype.storm.tuple.Values
import la.schema.PersonID
import la.speedlayer.UniquesOverTime.PageviewScheme
import la.speedlayer.CassandraState._
import storm.kafka.ZkHosts
import storm.kafka.trident.TransactionalTridentKafkaSpout
import storm.kafka.trident.TridentKafkaConfig
import storm.trident.Stream
import storm.trident.TridentTopology
import storm.trident.operation.BaseFunction
import storm.trident.operation.CombinerAggregator
import storm.trident.operation.TridentCollector
import storm.trident.operation.builtin.Count
import storm.trident.operation.builtin.Sum
import storm.trident.state.BaseStateUpdater
import storm.trident.state.StateFactory
import storm.trident.state.ValueUpdater
import storm.trident.state.map.MapState
import storm.trident.testing.MemoryMapState
import storm.trident.tuple.TridentTuple

import java.net.MalformedURLException
import java.net.URL
import java.util.ArrayList
import java.util.Iterator
import java.util.List

import scala.collection.JavaConversions._

object TridentSpeedLayer {
  class NormalizeURL extends BaseFunction {
    def execute(tuple: TridentTuple,
      collector: TridentCollector) = {
      try {
        val urlStr = tuple.getString(0)
        val url = new URL(urlStr)
        collector.emit(new Values(
          url.getProtocol() +
            "://" +
            url.getHost() +
            url.getPath()))
      } catch {
        case e:MalformedURLException => ()
      }
    }
  }

  class ToHourBucket extends BaseFunction {
    def execute(tuple: TridentTuple,
      collector: TridentCollector) = {
      val secs = tuple.getInteger(0)
      val hourBucket = secs / ToHourBucket.HOUR_SECS
      collector.emit(new Values(hourBucket.asInstanceOf[Object]))
    }
  }

  object ToHourBucket {
    val HOUR_SECS = 60 * 60
  }

  def pageviewsOverTime(): TridentTopology = {
    val topology = new TridentTopology()
    val kafkaConfig =
      new TridentKafkaConfig(
        new ZkHosts("127.0.0.1:2181"), "pageviews")
    kafkaConfig.scheme = new PageviewScheme()

    val opts = new CassandraState.Options[Any]()
    opts.keySerializer = StringSerializer.get()
    opts.colSerializer = IntegerSerializer.get()

    val state =
      CassandraState.transactional(
        "127.0.0.1",
        "superwebanalytics",
        "pageviewsOverTime",
        opts)

    val stream =
      topology.newStream(
        "pageviewsOverTime",
        new TransactionalTridentKafkaSpout(
          kafkaConfig))
        .each(new Fields("url"),
          new NormalizeURL(),
          new Fields("normurl"))
        .each(new Fields("timestamp"),
          new ToHourBucket(),
          new Fields("bucket"))
        .project(new Fields("normurl", "bucket"))
    stream.groupBy(new Fields("normurl", "bucket"))
      .persistentAggregate(
      state,
        new Count(),
        new Fields("count"))

    return topology
  }

  class ExtractDomain extends BaseFunction {
    def execute(tuple: TridentTuple,
      collector: TridentCollector) = {
      try {
        val url = new URL(tuple.getString(0))
        collector.emit(new Values(url.getAuthority()))
      } catch {
        case e:MalformedURLException => throw new RuntimeException(e)
      }
    }
  }

  class Visit(domain: String, user: PersonID) extends ArrayList[Object] {
    add(domain)
    add(user)
  }

  class VisitInfo(val startTimestamp: Int) {
    var lastVisitTimestamp = startTimestamp

    override def clone(): Object = {
      val ret = new VisitInfo(this.startTimestamp)
      ret.lastVisitTimestamp = this.lastVisitTimestamp
      ret
    }
  }

  object AnalyzeVisits {
    val LAST_SWEEP_TIMESTAMP = "lastSweepTs"
    val THIRTY_MINUTES_SECS = 30 * 60
  }
  class AnalyzeVisits
      extends BaseStateUpdater[MemoryMapState[Object]] {

    def updateState(
      state: MemoryMapState[Object],
      tuples: List[TridentTuple],
      collector: TridentCollector) = {
      tuples.foreach(t =>  {
        val domain = t.getString(0)
        val user = t.get(1).asInstanceOf[PersonID]
        val timestampSecs = t.getInteger(2)
        val v = new Visit(domain, user)
        update(state, v, new ValueUpdater[VisitInfo]() {
          def update(vi: VisitInfo): VisitInfo = {
            if(vi==null) {
              return new VisitInfo(timestampSecs)
            } else {
              val ret = new VisitInfo(vi.startTimestamp)
              ret.lastVisitTimestamp = timestampSecs
              return ret
            }
          }
        }.asInstanceOf[ValueUpdater[Object]])
        var lastSweep =
          get(state, AnalyzeVisits.LAST_SWEEP_TIMESTAMP).asInstanceOf[Integer]
        if(lastSweep==null) lastSweep = 0
        val expired = new ArrayList[Visit]()
        if(timestampSecs > lastSweep + 60) {
          val it = state.getTuples()
          while(it.hasNext()) {
            val tuple = it.next()
            if(!AnalyzeVisits.LAST_SWEEP_TIMESTAMP.equals(tuple.get(0))) {
              val visit = tuple.get(0).asInstanceOf[Visit]
              val info = tuple.get(1).asInstanceOf[VisitInfo]
              if(info!=null) {
                if(timestampSecs >
                  info.lastVisitTimestamp + AnalyzeVisits.THIRTY_MINUTES_SECS) {
                  expired.add(visit)
                  if(info.startTimestamp ==
                    info.lastVisitTimestamp) {
                    collector.emit(new Values(domain.asInstanceOf[Object], true.asInstanceOf[Object]))
                  } else {
                    collector.emit(new Values(domain.asInstanceOf[Object], false.asInstanceOf[Object]))
                  }
                }
              }
            }
          }
          put(state, AnalyzeVisits.LAST_SWEEP_TIMESTAMP, timestampSecs)
        }

        expired.foreach(visit => {
          remove(state, visit)
        })
      })
    }
  }

  def update(s: MapState[Object], key: Object, updater: ValueUpdater[Object]): Object = {
    val keys = new ArrayList[List[Object]]()
    val updaters = new ArrayList[ValueUpdater[Object]]()
    keys.add(new Values(key).asInstanceOf[List[Object]])
    updaters.add(updater)
    s.multiUpdate(keys, updaters.asInstanceOf[List[ValueUpdater[_]]]).get(0)
  }

  def get(s: MapState[Object], key: Object):  Object = {
    val keys = new ArrayList[List[Object]]()
    keys.add(new Values(key).asInstanceOf[List[Object]])
    return s.multiGet(keys).get(0)
  }

  def put(s: MapState[Object], key: Object, v: Object) = {
    val keys = new ArrayList[List[Object]]()
    keys.add(new Values(key).asInstanceOf[List[Object]])
    val vals = new ArrayList[Object]()
    vals.add(v)
    s.multiPut(keys, vals)
  }

  def remove(s: MemoryMapState[Object], key: Object) = {
    val keys = new ArrayList[List[Object]]()
    keys.add(new Values(key).asInstanceOf[List[Object]])
    s.multiRemove(keys)
  }

  class BooleanToInt extends BaseFunction {
    def execute(tuple: TridentTuple, collector:TridentCollector) {
      val v = tuple.getBoolean(0)
      if(v) {
        collector.emit(new Values(1.asInstanceOf[Object]))
      } else {
        collector.emit(new Values(0.asInstanceOf[Object]))
      }
    }
  }

  class CombinedCombinerAggregator(_aggs: CombinerAggregator[Any]*)
      extends CombinerAggregator[Any] {
    def init(tuple:TridentTuple): Object = {
      val ret = new ArrayList[Any]()
      _aggs.foreach(agg => ret.add(agg.init(tuple)))
      ret
    }

    def combine(o1:Any, o2:Any): Any = {
      val l1:List[Any] = o1.asInstanceOf[List[Any]]
      val l2:List[Any] = o2.asInstanceOf[List[Any]]
      val ret = new ArrayList[Any]()

      scala.List.range(0, _aggs.length).foreach(i => {
        ret.add(
          _aggs(i).combine(
            l1.get(i),
            l2.get(i)))
      })
      ret
    }

    def zero(): Object = {
      val ret = new ArrayList[Any]()
      _aggs.foreach(agg => ret.add(agg.zero()))
      ret
    }
  }


  def bounceRateOverTime(): TridentTopology = {
    val topology = new TridentTopology()
    val kafkaConfig =
      new TridentKafkaConfig(
        new ZkHosts(
          "127.0.0.1:2181"),
        "pageviews"
      )
    kafkaConfig.scheme = new PageviewScheme()

    val opts = new CassandraState.Options[Any]()
    opts.globalCol = "BOUNCE-RATE"
    opts.keySerializer = StringSerializer.get()
    opts.colSerializer = StringSerializer.get()

    topology.newStream(
      "bounceRate",
      new TransactionalTridentKafkaSpout(kafkaConfig))
      .each(new Fields("url"),
        new NormalizeURL(),
        new Fields("normurl"))
      .each(new Fields("normurl"),
        new ExtractDomain(),
        new Fields("domain"))
      .partitionBy(new Fields("domain", "user"))
      .partitionPersist(
      new MemoryMapState.Factory(),
        new Fields("domain", "user", "timestamp"),
        new AnalyzeVisits(),
        new Fields("domain", "isBounce"))
      .newValuesStream()
      .each(new Fields("isBounce"),
        new BooleanToInt(),
        new Fields("bint"))
      .groupBy(new Fields("domain"))
      .persistentAggregate(
      CassandraState.transactional(
        "127.0.0.1",
        "superwebanalytics",
        "bounceRate",
        opts),
        new Fields("bint"),
        new CombinedCombinerAggregator(
          new Count().asInstanceOf[CombinerAggregator[Any
          ]],
          new Sum().asInstanceOf[CombinerAggregator[Any]]).asInstanceOf[CombinerAggregator[Any]],
        new Fields("count-sum"))
    topology
  }

  def runPageviews(): LocalCluster = {
    val cluster = new LocalCluster()
    val conf = new Config()

    cluster.submitTopology("pageviews", conf, pageviewsOverTime().build())
    cluster
  }

  def runBounces(): LocalCluster = {
    val cluster = new LocalCluster()
    val conf = new Config()

    cluster.submitTopology("bounces", conf, bounceRateOverTime().build())
    cluster
  }
}
