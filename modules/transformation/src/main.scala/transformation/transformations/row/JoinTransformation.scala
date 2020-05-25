package transformation.transformations.row



//import transformation.{ParamValidator, ParamsTransformer, Transform, configurable}
//import transformation.transformation.errors.{TransformationError, UnexpectedError}
//import org.apache.spark.sql.DataFrame
//
//import scala.util.{Failure, Success, Try}
//import cats.implicits._
//import transformation.transformation.transformations.{JoinConfig, JoinTransformation}
//
//object JoinTransformation extends ParamsTransformer {
//  import Transform._
//  object JointransformationInstance extends JointransformationInstance
//  val selfAlias = "self"
//
//  trait JointransformationInstance {
//    implicit val JointransformationTransformation: Transform[JoinTransformation] = instance(
//      (op: JoinTransformation, df: DataFrame) => transformation(op, df)
//    )
//  }
//
//  //TODO: CHANGE ERROR HANDLING (UNEXPECTED ERRORS)
//  def transformation(op: JoinTransformation,
//                     startDf: DataFrame): Either[TransformationError, DataFrame] = {
//    val joinResult = joining(
//      startDf,
//      op.joins,
//      op.select,
//      op.resolveConflictsAuto
//    )
//
//    val selectResult = Try(joinResult.map(result => result.selectExpr(op.select: _*))) match {
//      case Failure(_) =>
//        UnexpectedError("Join", "JoinTransformation: 'select' contains unknown columns").asLeft
//      case Success(x) => x
//    }
//
//    val duplicatedColumns = selectResult.map(result => getDuplicatedColumns(result))
//
//    duplicatedColumns.flatMap { columns =>
//      columns.nonEmpty match {
//        case true if !op.resolveConflictsAuto =>
//          UnexpectedError(
//            "Join",
//            s"JoinTransformation: result contains ambiguous columns ${columns.mkString(", ")}").asLeft
//        case true if op.resolveConflictsAuto =>
//          val correctAmbiguity =
//            selectResult.map(df => resolveAmbiguity(op.joins, df, columns))
//          val duplicatedColumnsInCorrected = correctAmbiguity.map(ca => getDuplicatedColumns(ca))
//          duplicatedColumnsInCorrected.flatMap { duplicated =>
//            duplicated.nonEmpty match {
//              case true =>
//                UnexpectedError(
//                  "Join",
//                  s"JoinTransformation: There are duplicated columns in input.output " +
//                    s"that need to be resolved manually: ${duplicated.mkString(", ")}").asLeft
//              case false =>
//                correctAmbiguity
//            }
//          }
//        case false => selectResult
//      }
//    }
//  }
//
//  private def joining(
//      df: DataFrame,
//      joins: Seq[JoinConfig],
//      select: Seq[String],
//      resolveConflictsAuto: Boolean
//  ): Either[TransformationError, DataFrame] = {
//    joins.foldLeft(Right(df.alias(selfAlias)): Either[TransformationError, DataFrame])(
//      (df, joinConfig) => {
//
//        df.flatMap {
//          df =>
//            logger.debug(s"JoinTransformation: DF cols: ${df.columns.toList}")
//            logger.debug(
//              s"JoinTransformation: inputDf ${joinalias} cols: ${joininputDf.columns.toList}")
//
//            val inputDfWithAlias: Either[TransformationError, DataFrame] =
//              Right(joininputDf).map(df => df.alias(joinalias))
//
//            val joinExpression = Try(
//              inputDfWithAlias
//                .map(
//                  input =>
//                    joinjoinColumns
//                      .map(joinColumn => df.col(joinColumn.self) === input.col(joinColumn.other))
//                      .reduce(_ && _))) match {
//              case Failure(e) =>
//                UnexpectedError("Join", "'joinColumns' contains unknown columns").asLeft
//              case Success(x) => x
//            }
//            logger.info(
//              s"JoinTransformation: inputDfToJoin ${joinalias} type: ${joinjoinType} join : $joinExpression")
//            inputDfWithAlias.flatMap(input =>
//              joinExpression.map(joinExpression =>
//                df.join(input, joinExpression, joinjoinType)))
//        }
//      })
//  }
//
//  private def resolveAmbiguity(joins: Seq[JoinConfig],
//                               selectResult: DataFrame,
//                               duplicatedColumns: Set[String]): DataFrame = {
//    import utils.implicits.DataFrame._
//    selectResult.schema
//    (for {
//      joinConfig <- joins
//      column: String <- Try(selectResult.selectExpr(s"${joinalias}.*").columns)
//        .getOrElse(Array[String]())
//      if duplicatedColumns.contains(column)
//    } yield (joinalias, column)).foldLeft(selectResult) {
//      case (resolveConflictsDf, (alias, column)) =>
//        resolveConflictsDf
//          .withNestedColumn(s"${alias}_$column", resolveConflictsDf.col(s"$alias.$column"))
//          .drop(resolveConflictsDf.col(s"$alias.$column"))
//    }
//  }
//
//  private def getDuplicatedColumns(df: DataFrame): Set[String] =
//    df.columns.toList.groupBy(k => k).filter({ case (_, l) => l.size > 1 }).keySet
//}
