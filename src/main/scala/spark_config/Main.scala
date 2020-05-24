package spark_config

import input.inputs.InputUtils
import output.outputs.OutputUtils
import transformation.transformations.TransformationUtils
import com.typesafe.config.ConfigFactory
import java.nio.file.Paths

import input.Input._
import org.apache.spark.sql.{DataFrame, SparkSession}
import cats.implicits._
import transformation.errors.TransformationError
import input.Inputs._
import transformation.Transformations._
import output.Outputs._


object Main {
  def main(path: String): Unit = {

    val spark = SparkSession.builder().getOrCreate()

    val confFile = Paths.get(path).toFile
    val config = ConfigFactory.parseFile(confFile)

    val inputString = config.getAnyRef("input").toString()
    val outputString = config.getAnyRef("output").toString()
    val transformationsString = config.getAnyRefList("transformations")
    import scala.collection.JavaConverters._
    val transformationsString2 = transformationsString.asScala.map(x => x.toString())

    val input = InputUtils.getInput(inputString)
    val output = OutputUtils.getOutput(outputString)
    val transformations = TransformationUtils.getTransformations(transformationsString2)

    input.map(input => InputUtils.inputGeneric(spark)(input)
      .map(
        df =>
          transformations.map(
            transformations => transformations.foldLeft(Right(df): Either[TransformationError, DataFrame]) {
            (df_, transformation) =>
              df_.flatMap { dfnext =>
                TransformationUtils.applyTransformation(transformation)(dfnext)
              }
          }
            .map(
              df => {
                output.map(
                  output => OutputUtils.getGenericOutput(output)(df)
                ).leftMap(error => println(error))
              }
            )
      ).leftMap(error => println(error))
      ).leftMap(error => println(error))
    ).leftMap(error => println(error))
  }
}
