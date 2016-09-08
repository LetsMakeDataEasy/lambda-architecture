package com.thoughtworks.la

import org.scalatest.FunSuite
import com.thoughtworks.la.coretypes._
import com.thoughtworks.la.coretypes.conversions._

class ConverstionsTest extends FunSuite {
  test("File -> List[String]") {
    def myfun(lines: List[String]) = lines

    assert(myfun(ClasspathResource("/ab.txt")) == List("a", "b"))
  }
}
