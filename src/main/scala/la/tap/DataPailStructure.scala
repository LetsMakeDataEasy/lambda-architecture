package la.tap

import la.schema.Data

class DataPailStructure extends ThriftPailStructure[Data] {
  @Override
  def createThriftObject(): Data = new Data()

  def getType(): Class[_] = classOf[Data]
}
