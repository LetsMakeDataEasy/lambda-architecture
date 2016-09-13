package la.speedlayer

import backtype.storm.task.IMetricsContext
import backtype.storm.tuple.Values
import me.prettyprint.cassandra.serializers.BytesArraySerializer
import me.prettyprint.cassandra.serializers.IntegerSerializer
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate
import me.prettyprint.hector.api._
import me.prettyprint.hector.api.beans.HColumn
import me.prettyprint.hector.api.factory.HFactory
import storm.trident.state._
import storm.trident.state.Serializer
import storm.trident.state.map._

import java.io.Serializable
import java.util.ArrayList
import java.util.HashMap
import java.util.List
import java.util.Map

import scala.collection.JavaConversions._

class CassandraState[T](
  template: ThriftColumnFamilyTemplate[Any, Any],
  opts: CassandraState.Options[T],
  valueSer: Serializer[T]) extends IBackingMap[T] {

  @Override
  def multiGet(keys: List[List[Object]]): List[T] = {
    val ret = new ArrayList[T]
    keys.foreach(keyCol => {
      CassandraState.checkKeyCol(keyCol)
      val hcol = template.querySingleColumn(keyCol.get(0), getCol(keyCol), BytesArraySerializer.get())
      val v: T = null.asInstanceOf[T]
      if(hcol!=null) {
        ret.add(valueSer.deserialize(hcol.getValue()))
      }
      ret.add(v)
    })
    ret
  }

  @Override
  def multiPut(keys: List[List[Object]], vals: List[T]) = {
    scala.List.range(0, keys.size()).foreach(i => {
      val keyCol = keys.get(i)
      CassandraState.checkKeyCol(keyCol)
      val ser = valueSer.serialize(vals.get(i))
      val updater =
        template.createUpdater(keyCol.get(0))
      updater.setByteArray(getCol(keyCol), ser)
      template.update(updater)
    })
  }

  def getCol(keyCol: List[Object]): Object = {
    if(keyCol.size()==1) {
      opts.globalCol
    } else {
      keyCol.get(1)
    }
  }
}

object CassandraState {
  trait CassandraSerializerFactory extends Serializable {
    def getSerializer(): me.prettyprint.hector.api.Serializer[Any]
  }

  class StringSerializer extends CassandraSerializerFactory {
    def getSerializer():  me.prettyprint.hector.api.Serializer[Any] = {
      me.prettyprint.cassandra.serializers.StringSerializer.get().asInstanceOf[me.prettyprint.hector.api.Serializer[Any]]
    }

  }
  object StringSerializer {
    def get(): StringSerializer = {
      new StringSerializer()
    }
  }

  class IntegerSerializer extends CassandraSerializerFactory {
    def getSerializer(): me.prettyprint.hector.api.Serializer[Any] = {
      me.prettyprint.cassandra.serializers.IntegerSerializer.get().asInstanceOf[me.prettyprint.hector.api.Serializer[Any]]
    }

  }
  object IntegerSerializer {
    def get():  IntegerSerializer = {
      new IntegerSerializer()
    }

  }

  val DEFAULT_SERIALZERS = scala.collection.immutable.Map[StateType, Serializer[Any]](
    StateType.NON_TRANSACTIONAL -> new JSONNonTransactionalSerializer().asInstanceOf[Serializer[Any]],
    StateType.TRANSACTIONAL -> new JSONTransactionalSerializer().asInstanceOf[Serializer[Any]]
  )

  class Options[T] extends Serializable {
    var localCacheSize = 1000
    var globalKey: Object = "$GLOBAL$"
    var globalCol: Object = "$GLOBAL$"
    var keySerializer: CassandraSerializerFactory = StringSerializer.get()
    var colSerializer: CassandraSerializerFactory = StringSerializer.get()
    var valueSerializer: Serializer[T] = null
  }


  def transactional(connStr: String, keyspace: String, columnFamily: String): StateFactory ={
    transactional(connStr, keyspace, columnFamily, new Options())
  }

  def transactional(connStr: String, keyspace: String, columnFamily: String, opts: CassandraState.Options[Any]): StateFactory = {
    new Factory(connStr, keyspace, columnFamily, StateType.TRANSACTIONAL, opts)
  }

  def nonTransactional(connStr: String, keyspace: String, columnFamily: String): StateFactory = {
    nonTransactional(connStr, keyspace, columnFamily, new Options())
  }

  def nonTransactional(connStr: String, keyspace: String, columnFamily: String, opts: Options[Any]): StateFactory = {
    new Factory(connStr, keyspace, columnFamily, StateType.NON_TRANSACTIONAL, opts)
  }

  protected class Factory(
    connStr: String,
    keyspace: String,
    columnFamily: String,
    stateType: StateType,
    options: CassandraState.Options[Any]) extends StateFactory {
    val valueSer: Serializer[Any] = if(options.valueSerializer==null) {
      DEFAULT_SERIALZERS.get(stateType).get
    } else {
      options.valueSerializer
    }

    if(valueSer==null) {
      throw new RuntimeException("Couldn't find serializer for state type: " ++ stateType.toString)
    }

    @Override
    def makeState(conf: Map[_, _], context: IMetricsContext, partitionIndex: Int, numPartitions: Int): State = {
      val cluster = HFactory.getOrCreateCluster("mycluster", connStr)
      val _keyspace = HFactory.createKeyspace(keyspace, cluster)
      val template =
        new ThriftColumnFamilyTemplate(
          _keyspace,
          columnFamily,
          options.keySerializer.getSerializer(),
          options.colSerializer.getSerializer())

      val s = new CassandraState(template, options, valueSer)

      val c = new CachedMap(s, options.localCacheSize)
      val ms:MapState[Any] =
        if(stateType == StateType.NON_TRANSACTIONAL) {
          NonTransactionalMap.build(c.asInstanceOf[storm.trident.state.map.IBackingMap[Any]])
        } else if(stateType==StateType.TRANSACTIONAL){
          TransactionalMap.build(c.asInstanceOf[storm.trident.state.map.IBackingMap[storm.trident.state.TransactionalValue[_]]])
        } else {
          throw new RuntimeException("Unknown state type: " + stateType)
        }
      new SnapshottableMap(ms, new Values(options.globalKey))
    }

  }
  private def checkKeyCol(keyCol: List[Object]) = {
    if(keyCol.size()!=1 && keyCol.size()!=2) {
      throw new RuntimeException("Trident key should be a 2-tuple of key/column or a 1-tuple of key. Invalid: " + keyCol.toString())
    }
  }
}
