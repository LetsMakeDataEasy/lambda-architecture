package com.thoughtworks.la

import java.io.File
object conversions {
  implicit def fileToLines(f: File): List[String] = {
    scala.io.Source.fromFile(f).mkString.split("\n").toList
  }
}
