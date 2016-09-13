package la.tap

import java.util.ArrayList
import java.util.HashMap
import java.util.HashSet
import java.util.List
import java.util.Map
import la.schema.Data
import la.schema.DataUnit
import org.apache.thrift.TBase
import org.apache.thrift.TFieldIdEnum
import org.apache.thrift.TUnion
import org.apache.thrift.meta_data.FieldMetaData
import org.apache.thrift.meta_data.FieldValueMetaData
import org.apache.thrift.meta_data.StructMetaData

import scala.collection.JavaConversions._

class SplitDataPailStructure extends DataPailStructure {

  override def isValidTarget(dirs: String*): Boolean = {
    if(dirs.length==0) return false
    try {
      val id = dirs(0).toShort
      val s = SplitDataPailStructure.validFieldMap.get(id)
      if(s==null) return false
      else return s.isValidTarget(dirs: _*)
    } catch {
      case e:NumberFormatException => return false
    }
  }

  override def getTarget(o: Data): List[String] = {
    val ret: List[String] = new ArrayList[String]()
    val du = o.get_dataunit()
    val id = du.getSetField().getThriftFieldId()
    ret.add("" ++ id.toString)
    SplitDataPailStructure.validFieldMap.get(id).fillTarget(ret, du.getFieldValue())
    ret
  }
}

object SplitDataPailStructure {
  protected trait FieldStructure {
    def isValidTarget(dirs: String*): Boolean
    def fillTarget(ret: List[String], v: Object): Unit
  }

  val validFieldMap: HashMap[Short, FieldStructure]  = genFieldMap

  private def getMetadataMap(c: Class[_]): Map[TFieldIdEnum, FieldMetaData] = {
    try {
      val o = c.newInstance()
      c.getField("metaDataMap").get(o).asInstanceOf[Map[TFieldIdEnum, FieldMetaData]]
    } catch {
      case e: Exception => throw new RuntimeException(e)
    }
  }

  private def genFieldMap = {
    val result = new HashMap[Short, FieldStructure]()
    for( k <- DataUnit.metaDataMap.keySet()) {
      val md = DataUnit.metaDataMap.get(k).valueMetaData
      var fieldStruct: FieldStructure = null
      if(md.isInstanceOf[StructMetaData] && (md.asInstanceOf[StructMetaData])
        .structClass
        .getName()
        .endsWith("Property")) {
        fieldStruct = new PropertyStructure(md.asInstanceOf[StructMetaData] .structClass)
      } else {
        fieldStruct = new EdgeStructure()
      }
      result.put(k.getThriftFieldId(), fieldStruct)
    }
    result
  }

  protected class EdgeStructure extends FieldStructure {
    def isValidTarget(dirs: String*): Boolean = true
    def fillTarget(ret: List[String], v: Object): Unit = { }
  }

  protected class PropertyStructure(prop: Class[_]) extends FieldStructure {
    var valueId: TFieldIdEnum = null
    var validIds: HashSet[Short] = null
    try {
      val propMeta: Map[TFieldIdEnum, FieldMetaData]  = getMetadataMap(prop)
      val valClass: Class[_] = Class.forName(prop.getName() ++ "Value")
      valueId = getIdForClass(propMeta, valClass)

      validIds = new HashSet[Short]()
      val valMeta: Map[TFieldIdEnum, FieldMetaData] = getMetadataMap(valClass)
      valMeta.keySet().foreach {
        valId => validIds.add(valId.getThriftFieldId())
      }
    } catch {
      case e: Exception => throw new RuntimeException(e)
    }

    private def getIdForClass(meta: Map[TFieldIdEnum, FieldMetaData], toFind: Class[_]):  TFieldIdEnum = {
      for( k <- meta.keySet()) {
        val md = meta.get(k).valueMetaData
        if(md.isInstanceOf[StructMetaData]) {
          if(toFind.equals(md.asInstanceOf[StructMetaData].structClass)) {
            return k
          }
        }
      }
      throw new RuntimeException("Could not find " + toFind.toString() +
        " in " + meta.toString())
    }

    def isValidTarget(dirs: String*):  Boolean = {
      if(dirs.length<2) return false
      try {
        val s = dirs(1).toShort
        validIds.contains(s)
      } catch {
        case e: NumberFormatException => false
      }
    }

    def fillTarget(ret: List[String], v: Object) = {
      ret.add("" +
        v.asInstanceOf[TBase[_, TFieldIdEnum]]
        .getFieldValue(valueId).asInstanceOf[TUnion[_,_]]
        .getSetField()
        .getThriftFieldId())
    }
  }
}
