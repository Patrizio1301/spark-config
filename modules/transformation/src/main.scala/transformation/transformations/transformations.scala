package transformation.transformations

import com.typesafe.config.Config
import input.Input
import input.errors.InputError
import output.errors.OutputError
import org.apache.spark.sql.{DataFrame, SparkSession}
import pureconfig._
import pureconfig.error.ConfigReaderFailures
import pureconfig.generic.{FieldCoproductHint, ProductHint}
import transformation.Transform
import transformation.transformations.column._
import transformation.errors.TransformationError
import pureconfig.generic.auto._
import transformation.transformations.row.{ArithmeticoperationImp, RegexCaseColumnImp, SelectColumnsImp}
import utils.EitherUtils.EitherUtils
import transformation.{ParamValidator, Transform}
import cats.implicits._


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

case class DateFormatter
(
  field: String,
  format: String,
  reformat: Option[String] = None,
  locale: Option[String] = None,
  relocale: Option[String] = None,
  castMode: Option[String] = None,
  operation: Option[String] = None
) extends ParamValidator[DateFormatter]
  with ColumnTransformation
  with Transformation {
        def validate: Either[TransformationError, DateFormatter] =
                DateFormatterImp.validated(field, format, reformat, locale, relocale, castMode, operation)
}

case class ExtractInfoFromDate
(
  field: String,
  dateField: String,
  info: String
) extends ParamValidator[ExtractInfoFromDate]
  with ColumnTransformation
  with Transformation {
        def validate: Either[TransformationError, ExtractInfoFromDate] =
                ExtractInfoFromDateImp.validated(field, dateField, info)
}

/** Applies string replacements, if applicable, and changes the data type of the column
 */
case class Replacement
(
  pattern: String,
  replacement: String
)

case class Formatter
(
  field: String,
  typeToCast: String,
  replacements: Seq[Replacement] = Seq()
) extends ParamValidator[Formatter]
  with ColumnTransformation
  with Transformation {
        def validate: Either[TransformationError, Formatter] = ???
}

case class Hash
(
  field: String,
  hashType: String,
  hashLength: Option[Int] = None
) extends ParamValidator[Hash]
  with ColumnTransformation
  with Transformation {
        def validate: Either[TransformationError, Hash] =
                HashImp.validated(field, hashType, hashLength)
}

case class InsertLiteral
(
  field: String,
  offset: Int = 0,
  value: String,
  offsetFrom: Option[String] = None
) extends ParamValidator[InsertLiteral]
  with ColumnTransformation
  with Transformation {
        def validate: Either[TransformationError, InsertLiteral] =
                InsertLiteralImp.validated(field, offset, value, offsetFrom)
}

case class Integrity
(
  field: String,
  path: String,
  default: String
) extends ParamValidator[Integrity]
  with ColumnTransformation
  with Transformation {
        def validate: Either[TransformationError, Integrity] = ???
}

case class LeftPadding
(
  field: String,
  lengthDest: Option[Int] = None,
  fillCharacter: Option[String] = None,
  nullValue: Option[String] = None
) extends ParamValidator[LeftPadding]
  with ColumnTransformation
  with Transformation {
        def validate: Either[TransformationError, LeftPadding] = ???
}

case class Literal
(
  field: String,
  default: String,
  defaultType: Option[String] = None
) extends ParamValidator[Literal]
  with ColumnTransformation
  with Transformation {
        def validate: Either[TransformationError, Literal] = ???
}

case class Mask
(
  field: String,
  dataType: String
) extends ParamValidator[Mask]
  with ColumnTransformation
  with Transformation {
        def validate: Either[TransformationError, Mask] = ???
}

case class PartialInfo
(
  field: String,
  start: Option[Int] = None,
  length: Option[Int] = None,
  fieldInfo: String
) extends ParamValidator[PartialInfo]
  with ColumnTransformation
  with Transformation {
        def validate: Either[TransformationError, PartialInfo] =
                PartialInfoImp.validated(field, start, length, fieldInfo)
}

case class Replace
(
  field: String,
  replace: Map[String, String]
) extends ParamValidator[Replace]
  with ColumnTransformation
  with Transformation {
        def validate: Either[TransformationError, Replace] = ???
}

case class Trimmer
(
  field: String,
  trimType: Option[String] = None
) extends ParamValidator[Trimmer]
  with ColumnTransformation
  with Transformation {
        def validate: Either[TransformationError, Trimmer] =
                TrimmerImp.validated(field, trimType)
}

case class UpdateTime
(
  field: String
) extends ParamValidator[UpdateTime]
  with ColumnTransformation
  with Transformation {
        def validate: Either[TransformationError, UpdateTime] = ???
}

case class Arithmeticoperation(
                                valuesToOperateOn: Seq[String],
                                field: String,
                                operators: Seq[String]
                              ) extends ParamValidator[Arithmeticoperation]
  with Transformation {
        def validate: Either[TransformationError, Arithmeticoperation] =
                ArithmeticoperationImp.validated(valuesToOperateOn, field, operators)
}

case class Autocast(
                     fromType: String,
                     toType: String,
                     format: Option[String] = None,
                     exceptions: Seq[String] = Seq()
                   ) extends ParamValidator[Autocast]
  with Transformation {
        def validate: Either[TransformationError, Autocast] = ???
}

case class CleanNulls(
                       primaryKey: Option[Seq[String]] = None
                     ) extends ParamValidator[CleanNulls]
  with Transformation {
        def validate: Either[TransformationError, CleanNulls] = ???
}

case class ConcatColumns(
                          columnsToConcat: Seq[String],
                          columnName: String,
                          separator: Option[String] = None,
                          convertNulls: Option[String] = None
                        ) extends ParamValidator[ConcatColumns]
  with Transformation {

        def validate: Either[TransformationError, ConcatColumns] = ???
}

case class Expression(
                       condition: String,
                       field: Option[String] = None,
                       value: Option[String] = None
                     )
case class Conditional(
                        field: String,
                        default: String,
                        dataType: String,
                        expressions: Seq[Expression]
                      ) extends ParamValidator[Conditional]
  with Transformation {

        def validate: Either[TransformationError, Conditional] = ???
}

/**
 * DROPCOLUMNS TRANSFORMATION
 */
case class DropColumns(
                        columnsToDrop: Seq[String]
                      ) extends ParamValidator[DropColumns]
  with Transformation {
        def validate: Either[TransformationError, DropColumns] = ???
}


/** This class filter by custom criteria for a field.
 *
 * @param config filter by field.
 */
case class Filter(
                   field: String,
                   value: String,
                   op: String
                 )

case class FilterByField(
                          filters: Seq[Filter],
                          logicOp: Option[String] = None
                        ) extends ParamValidator[FilterByField]
  with Transformation {
        def validate: Either[TransformationError, FilterByField] = ???
}

/**
 * INITNULLS TRANSFORMATION
 */
case class InitNulls(
                      fields: Seq[String] = Seq(),
                      default: String,
                      defaultType: String,
                      field: Option[String] = None
                    ) extends ParamValidator[InitNulls]
  with Transformation {

        def validate: Either[TransformationError, InitNulls] = ???
}

/**
 * JOIN TRANSFORMATION
 */
//case class JoinColumn(self: String, other: String)
//case class JoinConfig(
//    alias: String,
//    joinType: String,
//    joinColumns: Seq[JoinColumn],
//    inputDf: DataFrame
//)
//
//case class JoinTransformation(
//    joins: Seq[JoinConfig],
//    select: Seq[String] = Seq("*"),
//    resolveConflictsAuto: Boolean = false
//) extends ParamValidator[JoinTransformation] {
//
//  def validate: Either[String, JoinTransformation] = ???
//}

/**
 * OFFSETCOLUMN TRANSFORMATION
 */
case class OffsetColumn(
                         offsetType: String,
                         columnToLag: String,
                         columnToOrderBy: String,
                         offset: Option[Int] = None,
                         newColumnName: Option[String] = None
                       ) extends ParamValidator[OffsetColumn]
  with Transformation {
        def validate: Either[TransformationError, OffsetColumn] = ???
}

/**
 * REGEXCASECOLUMN TRANSFORMATION
 */
case class RegexConfig(pattern: String, columnToRegex: String, value: String)
case class RegexCaseColumn(
                            field: String,
                            default: Option[String] = None,
                            regexList: Seq[RegexConfig]
                          ) extends ParamValidator[RegexCaseColumn]
  with Transformation {

        def validate: Either[TransformationError, RegexCaseColumn] =
                RegexCaseColumnImp.validated(field, default, regexList)
}

/**
 * REGEXCOLUMN TRANSFORMATION
 */
case class Regex(
                  regexGroup: Int,
                  field: String
                )

case class RegexColumn(
                        columnToRegex: String,
                        regexPattern: String,
                        regex: Seq[Regex] = Seq()
                      ) extends ParamValidator[RegexColumn]
  with Transformation {

        def validate: Either[TransformationError, RegexColumn] = ???
}

case class RenameColumns(
                          columnsToRename: Map[String, String]
                        ) extends ParamValidator[RenameColumns]
  with Transformation {

        def validate: Either[TransformationError, RenameColumns] = ???
}

case class SelectColumns(
                          columnsToSelect: Seq[String]
                        ) extends ParamValidator[SelectColumns]
  with Transformation {
        def validate: Either[TransformationError, SelectColumns] =
                SelectColumnsImp.validated(columnsToSelect)
}

case class SqlFilter(
                      filter: String
                    ) extends ParamValidator[SqlFilter]
  with Transformation {
        def validate: Either[TransformationError, SqlFilter] = ???
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