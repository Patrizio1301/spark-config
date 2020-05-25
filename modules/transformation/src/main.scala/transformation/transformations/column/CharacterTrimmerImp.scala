package transformation.transformations.column

import transformation._
import transformation.errors.{InvalidValue, TransformationError}
import cats.implicits._
import transformation.transformations.CharacterTrimmer
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import transformation.Transform._
import transformation.Parameters
import transformation.{Parameters, Transform}


/** This transformation returns a copy of the string, with leading or/and trailing specified character omitted.
  */
/**
  * Transform to replace columns values using a map
  */
object CharacterTrimmerImp extends Parameters {
  object CharacterTrimmerInstance extends CharacterTrimmerInstance

  trait CharacterTrimmerInstance {
    implicit val CharacterTrimmerTransformation: Transform[CharacterTrimmer] =
      instance((op: CharacterTrimmer, col: Column) => transformation(op, col))
  }

  /**
    * Custom trim transformation.
    *
    * @param col to be transformed by masterization.
    * @return Column transformed, if applies.
    */
  def transformation(op: CharacterTrimmer,
                             col: Column): Either[TransformationError, Column] = {
    val _trimType: String         = op.trimType.setDefault("left", "trimType")
    val _characterTrimmer: String = op.characterTrimmer.setDefault("0", "characterTrimmer")
    _trimType match {
      case "left" => regexp_replace(col, s"^${_characterTrimmer}+", "").asRight
      case "both" =>
        regexp_extract(col, s"^${_characterTrimmer}*(.*?)${_characterTrimmer}*$$", 1).asRight
      case "right" => regexp_replace(col, s"${_characterTrimmer}+$$", "").asRight
    }
  }

  def validated(
      field: String,
      trimType: Option[String] = None,
      characterTrimmer: Option[String] = None
  ): Either[TransformationError, CharacterTrimmer] = {

    for {
      validatedTrimType <- validateTrimType(trimType)
    } yield
      new CharacterTrimmer(
        field,
        validatedTrimType,
        characterTrimmer
      )
  }

  private def validateTrimType(
      trimType: Option[String]
  ): Either[TransformationError, Option[String]] =
    Either.cond(
      trimType match {
        case Some(value) => Seq("left", "right", "both").contains(value.toLowerCase())
        case None        => true
      },
      trimType match {
        case Some(value) => Some(value.toLowerCase())
        case None        => None
      },
      InvalidValue(
        "CharacterTrimmer",
        "trimType",
        trimType.get.toLowerCase()
      )
    )
}
