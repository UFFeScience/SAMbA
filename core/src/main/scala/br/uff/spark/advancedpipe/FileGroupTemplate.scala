package br.uff.spark.advancedpipe

import java.io.{File, IOException}
import java.nio.file.{Files, Path}
import java.util
import java.util.Collections
import java.util.function.Predicate

import br.uff.spark.utils.ScalaUtils._

import scala.beans.BeanProperty
import scala.collection.JavaConverters._

/**
  * @author Thaylon Guedes Santos
  */
object FileGroupTemplate {

  /* Utils */
  @throws[IOException]
  def ofIndividualFiles(files: List[File], keepBaseDir: Boolean, extraArgs: Map[String, Any]): List[FileGroupTemplate] =
    files.map((file: File) => ofFile(file, keepBaseDir, extraArgs))

  def ofFile(_file: File, keepBaseDir: Boolean, extraArgs: Map[String, Any]): FileGroupTemplate = {
    val file = _file.getAbsoluteFile
    var baseDir = file.getParentFile
    if (keepBaseDir) baseDir = null
    new FileGroupTemplate(baseDir, List(file), extraArgs)
  }

  def ofFiles(dir: File, files: List[String], extraArgs: Map[String, Any]): FileGroupTemplate =
    FileGroupTemplate.builder.baseDir(dir).files(files).extraInfos(extraArgs).build

  @throws[IOException]
  def ofDirectory(dir: File, isRecursive: Boolean, extraArgs: Map[String, Any]): FileGroupTemplate =
    ofDirectoryWithFilter(dir, isRecursive, (path: Path) => true, extraArgs)

  @throws[IOException]
  def ofDirectoryWithFilter(dir: File, isRecursive: Boolean, testFile: Predicate[Path], extraArgs: Map[String, Any]): FileGroupTemplate = {
    val maxDeep = if (isRecursive) Integer.MAX_VALUE else 1
    val files = Files.walk(dir.toPath, maxDeep)
      .filter(toJavaPredicate((path) => Files.isRegularFile(path)))
      .filter(toJavaPredicate((path) => testFile.test(path)))
      .iterator().asScala
      .map((path) => path.toFile).toList
    new FileGroupTemplate(dir, files, extraArgs)
  }

  /* Builder */
  def builder: FileGroupTemplateBuilder = new FileGroupTemplateBuilder

}

class FileGroupTemplate(
    @BeanProperty var baseDir: File,
    @BeanProperty var files: java.util.List[File],
    @BeanProperty var extrasInfo: java.util.Map[String, _] = Collections.emptyMap())
  extends Serializable {

  def this(baseDir: File, files: List[File], extrasInfo: Map[String, Any]) =
    this(baseDir, files.asJava, new util.HashMap[String, Any](extrasInfo.asJava))

  def allPaths: String = files.asScala.map(file => s"file://${file.getAbsolutePath}").mkString(",")

  var name: String = null

  def setName(newName: String): FileGroupTemplate = {
    name = newName
    this
  }

  def getName: String = {
    if (name == null) {
      name = files.asScala
        .map(f => f.getName)
        .sorted
        .map(name => name.replaceAllLiterally(".", "_"))
        .map(name => name.replaceAllLiterally(" ", "_"))
        .mkString(",")
    }
    name
  }

}
