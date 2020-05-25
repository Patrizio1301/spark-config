package transformation.transformations.row

import cats.implicits._
import utils.implicits.DataFrame._
import transformation._
import transformation.errors._
import transformation.Parameters
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.types.{DoubleType, IntegerType}
import org.apache.spark.sql.{Column, DataFrame}
import transformation.transformations.Arithmeticoperation
import transformation.{Parameters, Transform}

/** This class performs arithmetic operations on columns by
  * Currently restricted to +,-,/,*,%,^
  * NOTE: -, /, %, and ^ are not commutative operators, so column order in valuesToOperateOn will matter for these cases!
  *
  *  valuesToOperateOn: ["columns", "to", "operate", "on", "(required)" can be literal numeric values or column names]
  *  field: "column name for new column (required)"
  *  operators: ["List", "containing any of", "+", "-", "/", "*", "%", "or", "^", "(required)"]
  */
object ArithmeticoperationImp extends Parameters {
  import Transform._

  object ArithmeticoperationInstance extends ArithmeticoperationInstance

  trait ArithmeticoperationInstance {
    implicit val ArithmeticOperationTransformation: Transform[Arithmeticoperation] = instance(
      (op: Arithmeticoperation, df: DataFrame) => transformation(op, df)
    )
  }

  def transformation(op: Arithmeticoperation,
                     df: DataFrame): Either[TransformationError, DataFrame] = {

    logger.info(s"ArithmeticOperation: Applying operators: '${op.operators
      .mkString(",")}' to columns: '${op.valuesToOperateOn.mkString(",")}'")

    val literalNumericColumnsToAdd = determineLiteralColumnsNeeded(df.columns, op.valuesToOperateOn)
    val dfWithLiterals             = addLiteralColumnsIfNeeded(df, literalNumericColumnsToAdd)

    val escapedColumns = op.valuesToOperateOn.map(colName =>
      if (literalNumericColumnsToAdd.contains(colName)) {
        col(s"temp_${findPower(colName.toFloat)}")
      } else col(s"$colName"))

    val colHead           = escapedColumns.head
    val colTail           = escapedColumns.tail
    val preparedOperators = prepareOperatorsToZip(op.operators, colTail.length)
    val colWithOperator   = colTail.zip(preparedOperators)

    val finalColumn =
      colWithOperator.foldLeft(Right(colHead): Either[TransformationError, Column]) {
        (tmpCol, colOpPair) =>
          val valueToOperateOn   = colOpPair._1
          val operationToPerform = colOpPair._2
          operationToPerform match {
            case "+" => tmpCol.map(col => col + valueToOperateOn)
            case "-" => tmpCol.map(col => col - valueToOperateOn)
            case "*" => tmpCol.map(col => col * valueToOperateOn)
            case "/" => tmpCol.map(col => col / valueToOperateOn)
            case "%" => tmpCol.map(col => col % valueToOperateOn)
            case "^" => tmpCol.map(col => pow(col, valueToOperateOn))
            case _ =>
              Left(OperationError(
                "ArithmeticOperation",
                s"Operation unspecified or not allowed. Available operations are +, -, *, /, %, ^"))
          }
      }
    finalColumn.map(column =>
      dfWithLiterals
        .withNestedColumn(op.field, column)
        .drop(literalNumericColumnsToAdd.map(colName => s"temp_${findPower(colName.toFloat)}"): _*))
  }

  private def determineLiteralColumnsNeeded(
      cols: Array[String],
      valuesToOperateOn: Seq[String]
  ): Seq[String] = {
    valuesToOperateOn.filter(
      colName =>
        colName.matches("^[+-]?([0-9]+(\\.[0-9]+)?|\\.[0-9]+)$")
          && !cols.contains(colName))
  }

  private def addLiteralColumnsIfNeeded(df: DataFrame, colsToAdd: Seq[String]): DataFrame = {
    val numericType =
      if (colsToAdd.exists(columnName => columnName.contains("."))) DoubleType else IntegerType
    colsToAdd.foldLeft(df) { (curDF, columnToAdd) =>
      import utils.implicits.DataFrame._
      curDF.withNestedColumn(s"temp_${findPower(columnToAdd.toFloat)}",
                             lit(columnToAdd).cast(numericType))
    }
  }

  private def prepareOperatorsToZip(operatorList: Seq[String], colLength: Int): Seq[String] = {
    if (colLength > operatorList.length) {
      val operatorToPadWith = operatorList.takeRight(1).head
      operatorList.padTo(colLength, operatorToPadWith)
    } else if (colLength < operatorList.length) {
      logger.warn(
        s"There was an equal or greater number of operators than values to operate on! Truncating to '$colLength' operators.")
      operatorList.slice(0, colLength)
    } else {
      operatorList
    }
  }

  /**
    * This functions establishes a biyection between the numbers and the temp names dropping decimal points and number signs.
    * Por example, since -25.3 and 2.53 whould be both temp_253, we add the maximal power of 10 the
    * number is contained and flag the sign with 0 (negative) and 1 (positive).
    * Since |-25.3| is contained in the interval [10*^1,10^2],we add a 2 at the end and since -25.3 is negative,
    * we add a 0. Thus, -25.3 will be temp_25320.
    * For 2.53 we add a 1 since |2.53| is contained in the interval [10*^0,10^1] and a 1 since the number is positive.
    * Thus, 2.53 will be temp_25311.
    */
  private def findPower(number: Float): String = {
    val sign       = if (Math.abs(number) < 1) -1 else 1
    var power: Int = 0
    while (Math.pow(10, power * sign) < Math.abs(number)) { power += 1 }
    s"${number.toString.replaceAll("[\\-\\+\\.]", "")}$power${if (sign == 1) 1 else 0}${if (Math.abs(number) == number) 1
    else 0}"
  }

  def validated(
      valuesToOperateOn: Seq[String],
      field: String,
      operators: Seq[String]
  ): Either[TransformationError, Arithmeticoperation] = {

    for {
      validatedValuesToOperateOn <- validateValuesToOperateOn(valuesToOperateOn)
      validatedOperators         <- validateOperators(valuesToOperateOn)

    } yield
      new Arithmeticoperation(
        validatedValuesToOperateOn,
        field,
        validatedOperators
      )
  }

  private def validateValuesToOperateOn(
      valuesToOperateOn: Seq[String]
  ): Either[TransformationError, Seq[String]] =
    Either.cond(
      valuesToOperateOn.nonEmpty && valuesToOperateOn.size != 1,
      valuesToOperateOn,
      EmptyError(
        "ArithmeticOperation",
        "valuesToOperateOn",
        "These operations cannot be performed on one or zero columns." +
          " Add two or more column names into valuesToOperateOn: [<column1>, <column2>]"
      )
    )

  private def validateOperators(
      operators: Seq[String]
  ): Either[TransformationError, Seq[String]] =
    Either.cond(
      operators.nonEmpty,
      operators,
      EmptyError(
        "ArithmeticOperation",
        "operators",
        "There must be at least one operator to perform an operation." +
          " Available operations are +, -, *, /, %, ^"
      )
    )
}
