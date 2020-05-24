package transformation.transformations

import com.typesafe.config.Config
import input.Input
import input.errors.InputError
import org.apache.spark.sql.{DataFrame, SparkSession}
import output.errors.OutputError
import pureconfig._
import pureconfig.error.ConfigReaderFailures
import pureconfig.generic.{FieldCoproductHint, ProductHint}
import transformation.{ParamValidator, Transform}
import transformation.transformations.column._
import transformation.errors.TransformationError
import pureconfig.generic.auto._
import utils.EitherUtils.EitherUtils


sealed trait Transformation

case class Base64
(
  field: String,
  encrypted: Option[Boolean] = None
) extends ParamValidator[Base64]
  with ColumnTransformation
  with Transformation {
        def validate: Either[TransformationError, Base64] = ???
}

case class CaseLetter
(
  field: String,
  operation: String
) extends ParamValidator[CaseLetter]
  with ColumnTransformation
  with Transformation {
        def validate: Either[TransformationError, CaseLetter] =
                CaseLetterImp.validated(field, operation)
}

case class Catalog
(
  field: String,
  path: String
) extends ParamValidator[Catalog]
  with ColumnTransformation
  with Transformation {
        def validate: Either[TransformationError, Catalog] = ???
}

case class CharacterTrimmer
(
  field: String,
  trimType: Option[String] = None,
  characterTrimmer: Option[String] = None
) extends ParamValidator[CharacterTrimmer]
  with ColumnTransformation
  with Transformation {
        def validate: Either[TransformationError, CharacterTrimmer] =
                CharacterTrimmerImp.validated(field, trimType, characterTrimmer)
}

case class CommaDelimiter
(
  field: String,
  lengthDecimal: Option[Int] = None,
  separatorDecimal: Option[String] = None
) extends ParamValidator[CommaDelimiter]
  with ColumnTransformation
  with Transformation {
        def validate: Either[TransformationError, CommaDelimiter] = ???
}

case class CopyColumn
(
  field: String,
  copyField: String,
  defaultType: Option[String] = None
) extends ParamValidator[CopyColumn]
  with ColumnTransformation
  with Transformation {
        def validate: Either[TransformationError, CopyColumn] = ???
}

object TransformationUtils {
        def getTransformation[T<:Transformation](config: String): Either[ConfigReaderFailures, Transformation] = {
                implicit val hint = ProductHint[Transformation](useDefaultArgs = true)
                ConfigSource.string(config).load[Transformation]
        }

        def getTransformations(config: Seq[String]): Either[ConfigReaderFailures, Seq[Transformation]] = {
                EitherUtils.sequence(config.map(conf => getTransformation(conf)))
        }

        def applyTransformation[C](transformation: C)(df: DataFrame)(
        implicit transform: Transform[C]): Either[TransformationError, DataFrame] =
        transform.transform(transformation)(df)
}