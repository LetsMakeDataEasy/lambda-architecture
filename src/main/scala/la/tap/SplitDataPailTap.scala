package la.tap

import backtype.cascading.tap.PailTap
import backtype.cascading.tap.PailTap.PailTapOptions
import backtype.hadoop.pail.PailSpec
import backtype.hadoop.pail.PailStructure
import java.util.ArrayList
import java.util.List
import la.schema.DataUnit
import org.apache.thrift.TFieldIdEnum
import scala.collection.JavaConversions._

class SplitDataPailTap(
  root: String,
  options: SplitDataPailTap.SplitDataPailTapOptions = new SplitDataPailTap.SplitDataPailTapOptions(),
  attrs: Array[_] = null.asInstanceOf[Array[DataUnit._Fields]])
    extends PailTap(
  root, new PailTapOptions(
    PailTap.makeSpec(
      options.spec,
      SplitDataPailTap.getSpecificStructure),
    options.fieldName,
    SplitDataPailTap.toAttrs(attrs),
    null)
)

object SplitDataPailTap {
  case class SplitDataPailTapOptions(spec: PailSpec = null, fieldName: String = "data")

  def getSpecificStructure(): PailStructure[_] = new SplitDataPailStructure()

  def makeAttrs[T](spec: Array[T])(f: T => String): Array[List[String]] = {
    if(spec==null) return null
    scala.List.range(0, spec.length).map(x => seqAsJavaList(scala.List(f(spec(x))))).toArray
  }

  def toAttrs[T](spec: Array[T]): Array[List[String]] = spec match {
    case _: Array[DataUnit._Fields] => makeAttrs(spec)(_.getThriftFieldId.toString)
    case _: Array[Int] => makeAttrs(spec)(_.toString)
    case _: Array[List[TFieldIdEnum]] => makeAttrs(spec)(_.map(_.getThriftFieldId.toString).mkString)
  }
}
