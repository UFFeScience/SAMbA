import java.io.File
import java.util.Scanner
import java.util.concurrent.TimeUnit

import br.uff.spark.advancedpipe.FileGroupTemplate
import br.uff.spark.advancedpipe.{FileGroup, FileGroupTemplate}
import br.uff.spark.schema.SingleLineSchema
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.io.Source
import scala.util.control.Breaks._

object Main {

  val WORKSPACE = System.getenv("WORKSPACE")

  def parserInputFile(fileLocation: String): Seq[FileGroupTemplate] = {
    val scanner = new Scanner(new File(fileLocation))
    scanner.nextLine()
    val list = new ArrayBuffer[FileGroupTemplate]()
    while (scanner.hasNext) {
      val data = scanner.nextLine().split(";")
      val name = data(0)
      val filePath = data(1)
      list += FileGroupTemplate.ofFile(new File(filePath), false, Map("NAME" -> name))
    }
    return list
  }

  def main(args: Array[String]) {

    val sc = new SparkContext(
      new SparkConf().setAppName("SciPhy")
        .setScriptDir(WORKSPACE+"/scripts")
    )

    sc.fileGroup(parserInputFile(args(0)): _*)
      .runScientificApplication("mafft.cmd {{NAME}}").setName("Mafft")
      .runScientificApplication("readseq.cmd {{NAME}} {{NAME}}.mafft").setName("Readseq").setSchema(new ReadSeqSchema)
      .runScientificApplication("modelgenerator.cmd {{NAME}}").setName("Model Generator").setSchema(new ModelGeneratorSchema)
      .runScientificApplication("raxml.cmd {{NAME}}.phylip {{NAME}}.mg.modelFromMG.txt").setName("Raxml").setSchema(new RaxmlSchema)
      .saveFilesAt(new File(WORKSPACE+"/output"))

    sc.stop()
  }

}

class ReadSeqSchema extends SingleLineSchema[FileGroup] {

  override def getFieldsNames(): Array[String] = Array("FILE_NAME", "NUM_ALIGNS", "LENGTH")

  override def splitData(value: FileGroup): Array[String] = {
    val file = value.getFileElements.filter(file => file.getFileName.endsWith(".phylip")).head
    val line = Source.fromInputStream(file.getContents.toInputStream).getLines().next()
    val data = line.trim.split(" ")
    Array(file.getFileName, data(0), data(1))
  }

}

class ModelGeneratorSchema extends SingleLineSchema[FileGroup] {

  override def getFieldsNames(): Array[String] = Array("MG_FILE_NAME", "MODEL1", "PROB1", "MODEL2", "PROB2")

  val template = "****Akaike Information Criterion "
  val modelTemplate = "Model Selected: "
  val probabilityTemplate = "-lnL = "

  override def splitData(value: FileGroup): Array[String] = {
    val file = value.getFileElements.filter(file => file.getFileName == "modelgenerator0.out").head
    val scanner = new Scanner(file.getContents.toInputStream)

    val criteria = new ListBuffer[(String, String)]()

    var line = scanner.nextLine()
    breakable {
      while (line != null) {
        var model = ""
        var probability = ""
        if (line.startsWith(template)) {
          line = scanner.nextLine()
          breakable {
            while (line != null) {
              if (line.startsWith(modelTemplate)) model = line.replaceAll(modelTemplate, "")
              else if (line.startsWith(probabilityTemplate)) probability = line.replaceAll(probabilityTemplate, "")
              if ((!model.isEmpty) && (!probability.isEmpty)) {
                criteria += ((model, probability))
                break
              }
              line = scanner.nextLine()
            }
          }
          if (criteria.length >= 2)
            break
        } else {
          line = if (scanner.hasNext()) scanner.nextLine() else null
        }
      }
    }
    scanner.close()
    Array(
      value.getExtrasInfo.get("NAME").asInstanceOf[String] + ".mg.modelFromMG.txt",
      criteria(0)._1, criteria(0)._2,
      criteria(1)._1, criteria(1)._2
    )
  }

}

class RaxmlSchema extends SingleLineSchema[FileGroup] {

  override def getFieldsNames(): Array[String] = Array("BEST_TREE", "BEST_SCORE")

  val template = "Final GAMMA-based Score of best tree "

  override def splitData(value: FileGroup): Array[String] = {
    val fileName = value.getExtrasInfo.get("NAME").asInstanceOf[String]
    val file = value.getFileElements.filter(file => file.getFileName == s"RAxML_info.${fileName}.phylip_raxml_tree1.singleTree").head
    val scanner = new Scanner(file.getContents.toInputStream)

    var bestScore = "ERROR"
    breakable {
      while (scanner.hasNext) {
        val line = scanner.nextLine()
        if (line.startsWith(template)) {
          bestScore = line.replaceAll(template, "")
          break
        }
      }
    }
    scanner.close()

    Array(s"RAxML_bipartitions.${fileName}.phylip_tree3.BS_TREE", bestScore.trim)
  }

}
