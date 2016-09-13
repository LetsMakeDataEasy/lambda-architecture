package la.speedlayer

import backtype.storm.Config
import backtype.storm.Constants
import backtype.storm.LocalCluster
import backtype.storm.spout.MultiScheme
import backtype.storm.task.OutputCollector
import backtype.storm.task.TopologyContext
import backtype.storm.topology.BasicOutputCollector
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.TopologyBuilder
import backtype.storm.topology.base.BaseBasicBolt
import backtype.storm.topology.base.BaseRichBolt
import backtype.storm.tuple.Fields
import backtype.storm.tuple.Tuple
import backtype.storm.tuple.Values
import com.clearspring.analytics.stream.cardinality.HyperLogLog
import kafka.javaapi.producer.Producer
import kafka.message.Message
import kafka.producer.KeyedMessage
import kafka.producer.ProducerConfig
import la.schema.Data
import la.schema.PageViewEdge
import la.schema.PersonID
import me.prettyprint.cassandra.serializers.BytesArraySerializer
import me.prettyprint.cassandra.serializers.IntegerSerializer
import me.prettyprint.cassandra.serializers.LongSerializer
import me.prettyprint.cassandra.serializers.StringSerializer
import me.prettyprint.cassandra.service.template._
import me.prettyprint.hector.api.Cluster
import me.prettyprint.hector.api.Keyspace
import me.prettyprint.hector.api.beans.HColumn
import me.prettyprint.hector.api.factory.HFactory
import org.apache.thrift.TDeserializer
import org.apache.thrift.TException
import org.apache.thrift.TSerializer
import storm.kafka.KafkaConfig
import storm.kafka.ZkHosts
import storm.kafka.KafkaSpout
import storm.kafka.SpoutConfig
import java.io.IOException
import java.net.MalformedURLException
import java.net.URL
import java.util.ArrayList
import java.util.List
import java.util.Map
import java.util.Properties

import la.test.Data.makeEquiv
import la.test.Data.makePageview

import scala.collection.JavaConversions._

object UniquesOverTime {

  def initTestData(): Unit = {
    val data = new ArrayList[Data]()
    data.add(makePageview(1, "http://foo.com/post1", 60))
    data.add(makePageview(2, "http://foo.com/post1", 60))
    data.add(makePageview(3, "http://foo.com/post1", 62))
    data.add(makePageview(2, "http://foo.com/post3", 62))
    data.add(makePageview(1, "http://foo.com/post1", 4000))
    data.add(makePageview(1, "http://foo.com/post2", 4000))
    data.add(makePageview(1, "http://foo.com/post2", 10000))
    data.add(makePageview(5, "http://foo.com/post3", 10600))

    val props = new Properties()

    props.put("metadata.broker.list", "127.0.0.1:9092")
    props.put("serializer.class", "kafka.serializer.DefaultEncoder")

    val config = new ProducerConfig(props)

    val ser = new TSerializer()

    val producer = new Producer[String, Array[Byte]](config)
    data.foreach(d => {
      val m = new KeyedMessage[String, Array[Byte]]("pageviews", null, ser.serialize(d))
      producer.send(m)
    })

    producer.close
  }

  class PageviewScheme extends MultiScheme {
    var _des: TDeserializer = null

    @Override
    def deserialize(bytes: Array[Byte]): List[List[Object]] = {
      val data = new Data()
      if(_des==null) _des = new TDeserializer()
      try {
        _des.deserialize(data, bytes)
      } catch {
        case e:TException => throw new RuntimeException(e)
      }
      val pageview = data.get_dataunit().get_page_view()
      val url = pageview.get_page().get_url()
      val user = pageview.get_person()
      val ret = new ArrayList[List[Object]]()
      ret.add(new Values(user.asInstanceOf[Object],
        url.asInstanceOf[Object],
        data.get_pedigree()
          .get_true_as_of_secs().asInstanceOf[Object]).asInstanceOf[List[Object]])
      ret
    }

    @Override
    def getOutputFields(): Fields = {
      new Fields("user", "url", "timestamp")
    }
  }

  object ExtractFilterBolt {
    val HOUR_SECS = 60 * 60
  }
  class ExtractFilterBolt extends BaseBasicBolt {
    @Override
    def execute(tuple: Tuple, collector: BasicOutputCollector) = {
      val user = tuple.getValue(0).asInstanceOf[PersonID]
      val url = tuple.getString(1)
      val timestamp = tuple.getInteger(2)
      try {
        val domain = new URL(url).getAuthority()
        collector.emit(new Values(
          domain.asInstanceOf[Object],
          url.asInstanceOf[Object],
          (timestamp / ExtractFilterBolt.HOUR_SECS).asInstanceOf[Object],
          user.asInstanceOf[Object]))
      } catch {
        case e:MalformedURLException => ()
      }
    }

    @Override
    def declareOutputFields(declarer: OutputFieldsDeclarer) = {
      declarer.declare(new Fields("domain", "url", "bucket", "user"))
    }
  }

  class UpdateCassandraBolt extends BaseBasicBolt {

    var _template:ColumnFamilyTemplate[String, Integer] = null

    override def prepare(conf: Map[_, _], context: TopologyContext) = {
      val cluster = HFactory.getOrCreateCluster(
        "mycluster", "127.0.0.1")

      val keyspace = HFactory.createKeyspace(
        "superwebanalytics", cluster)

      _template =
        new ThriftColumnFamilyTemplate[String, Integer](
          keyspace,
          "uniques",
          StringSerializer.get(),
          IntegerSerializer.get())
    }

    @Override
    def execute(tuple:Tuple, collector:BasicOutputCollector) = {
      val url = tuple.getString(1)
      val bucket = tuple.getInteger(2)
      val user = tuple.getValue(3).asInstanceOf[PersonID]
      val hcol = _template.querySingleColumn(
        url,
        bucket,
        BytesArraySerializer.get())
      var hll: HyperLogLog = null
      try {
        if(hcol==null) hll = new HyperLogLog(14)
        else hll = HyperLogLog.Builder.build(hcol.getValue())
        hll.offer(user)
        val updater =
          _template.createUpdater(url)
        updater.setByteArray(bucket, hll.getBytes())
        _template.update(updater)
      } catch{
        case e:IOException =>
          throw new RuntimeException(e)
      }
    }

    @Override
    def declareOutputFields(declarer: OutputFieldsDeclarer) = {
    }
  }

  def run(): LocalCluster = {
    val builder = new TopologyBuilder()
    val spoutConfig = new SpoutConfig(
      new ZkHosts("127.0.0.1:2181"),
      "pageviews",
      "/kafkastorm",
      "uniquesSpeedLayer"
    )
    spoutConfig.scheme = new PageviewScheme()

    builder.setSpout("pageviews",
      new KafkaSpout(spoutConfig), 2)

    builder.setBolt("extract-filter",
      new ExtractFilterBolt(), 4)
      .shuffleGrouping("pageviews")
    builder.setBolt("cassandra",
      new UpdateCassandraBolt(), 4)
      .fieldsGrouping("extract-filter",
        new Fields("domain"))

    val cluster = new LocalCluster()
    val conf = new Config()
    cluster.submitTopology("uniques", conf, builder.createTopology())

    cluster
  }
}
