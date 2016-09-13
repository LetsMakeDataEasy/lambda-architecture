package la.tap

import backtype.cascading.tap.PailTap
import backtype.cascading.tap.PailTap.PailTapOptions
import backtype.hadoop.pail.PailSpec
import backtype.hadoop.pail.PailStructure


class DataPailTap(root: String, options: DataPailTap.DataPailTapOptions)
    extends PailTap(root, new PailTapOptions(
      PailTap.makeSpec(
        options.spec,
        DataPailTap.getSpecificStructure()),
      options.fieldName, null, null)) {

  def this(root: String) {
    this(root, new DataPailTap.DataPailTapOptions)
  }
}

object DataPailTap {
  case class DataPailTapOptions(spec: PailSpec = null, fieldName: String = "data")
  def getSpecificStructure(): PailStructure[_] = new DataPailStructure()
}
