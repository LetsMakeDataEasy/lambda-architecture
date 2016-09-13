package la.tap;

import backtype.hadoop.pail.PailStructure;
import java.util.Collections;
import java.util.List;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

trait ThriftPailStructure[T <: Comparable[_]]
  extends PailStructure[T]
{
  protected def createThriftObject(): T

  var des: TDeserializer = null

  def getDeserializer(): TDeserializer = {
    if(des==null) des = new TDeserializer()
    des
  }

  def deserialize(record: Array[Byte]): T = {
    val ret = createThriftObject()
    try {
      getDeserializer().deserialize(ret.asInstanceOf[TBase[_, _]], record)
    } catch {
      case e:TException => throw new RuntimeException(e)
    }
    ret
  }

  var ser: TSerializer = null

  def getSerializer(): TSerializer = {
    if(ser==null) ser = new TSerializer()
    ser
  }

  def serialize(obj: T): Array[Byte] = {
    try {
      return getSerializer().serialize(obj.asInstanceOf[TBase[_, _]]);
    } catch {
      case e: TException => throw new RuntimeException(e)
    }
  }

  def isValidTarget(dirs: String*): Boolean = true

  def getTarget(o: T): List[String] = Collections.EMPTY_LIST.asInstanceOf[List[String]]
}
