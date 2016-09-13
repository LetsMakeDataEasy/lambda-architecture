package la.test

import la.schema._

object Data {
  def makePedigree(timeSecs: Int):  Pedigree = {
    new Pedigree(timeSecs,
      Source.SELF,
      OrigSystem.page_view(new PageViewSystem())
    )
  }

  def makePageview(userid: Int, url: String, timeSecs: Int): la.schema.Data = {
    new la.schema.Data(makePedigree(timeSecs),
      DataUnit.page_view(
        new PageViewEdge(
          PersonID.user_id(userid),
          PageID.url(url),
          1
        )))
  }

  def makeEquiv(user1: Int, user2: Int): la.schema.Data = {
    new la.schema.Data(makePedigree(1000),
      DataUnit.equiv(
        new EquivEdge(
          PersonID.user_id(user1),
          PersonID.user_id(user2)
        )))
  }
}
