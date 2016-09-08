package com.thoughtworks.la.base.coretypes

case class ClasspathResource(name: String)

object conversions {
  import java.io.File

  implicit def classpathResourceToFile(cpr: ClasspathResource): java.io.File = {
    new java.io.File(getClass.getResource(cpr.name).getPath)
  }

  implicit def fileToLines(f: File): List[String] = {
    scala.io.Source.fromFile(f).mkString.split("\n").toList
  }

  implicit def classpathResrouceToLines(cpr: ClasspathResource): List[String] = {
    fileToLines(cpr)
  }

}
