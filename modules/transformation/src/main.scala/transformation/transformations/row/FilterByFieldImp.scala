package transformation.transformations.row

import transformation._
import transformation.errors.{InvalidValue, TransformationError, UnexpectedError}
import org.apache.spark.sql.{Column, DataFrame}
import cats.implicits._
import transformation.transformations.FilterByField
import transformation.CommonLiterals._
import transformation.Transform
import transformation.{Parameters, Transform}

import scala.util.{Failure, Success, Try}

object FilterByFieldImp extends Parameters {

  import Transform._

  object FilterByFieldInstance extends FilterByFieldInstance

  trait FilterByFieldInstance {
    implicit val FilterByFieldTransformation: Transform[FilterByField] = instance(
      (op: FilterByField, df: DataFrame) => transformation(op, df)
    )
  }

  def transformation(op: FilterByField, df: DataFrame): Either[TransformationError, DataFrame] = {
    val _logicOp: String = op.logicOp.setDefault(AND_OP, "logicOp")
    val conditionList =
      op.filters.foldLeft(Right(Seq[Column]()): Either[TransformationError, Seq[Column]]) {
        (seq, cfg) =>
          val rawFieldName  = cfg.field.toColumnName
          val column        = df.col(rawFieldName)
          val op            = cfg.op
          val value: AnyRef = cfg.value

          logger.info(s"${getClass.getSimpleName}: applying filter $column $op $value")

          op.toLowerCase match {
            case EQ_OP        => seq.map(sequence => sequence ++ Seq(column.equalTo(value)))
            case NEQ_OP       => seq.map(sequence => sequence ++ Seq(column.notEqual(value)))
            case LT_OP        => seq.map(sequence => sequence ++ Seq(column.lt(value)))
            case LEQ_OP       => seq.map(sequence => sequence ++ Seq(column.leq(value)))
            case GT_OP        => seq.map(sequence => sequence ++ Seq(column.gt(value)))
            case GEQ_OP       => seq.map(sequence => sequence ++ Seq(column.geq(value)))
            case LIKE_OP      => seq.map(sequence => sequence ++ Seq(column.like(cfg.value)))
            case RLIKE_OP     => seq.map(sequence => sequence ++ Seq(column.rlike(cfg.value)))
            case invalidValue => InvalidValue("FilterByField", "op", invalidValue, "").asLeft
          }
      }
    conditionList.flatMap { _conditionalList =>
      Try(
        _logicOp match {
          case AND_OP => df.filter(_conditionalList.reduce((c1, c2) => c1 && c2))
          case OR_OP  => df.filter(_conditionalList.reduce((c1, c2) => c1 || c2))
        }
      ) match {
        case Success(dataFrame) => dataFrame.asRight
        case Failure(e)         => UnexpectedError("FilterByField", e.getMessage).asLeft
      }
    }
  }
}
