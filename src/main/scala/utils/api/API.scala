package utils.api

import transformation.errors.TransformationError
class API {

  def getError[L, R](either: Either[TransformationError, R]): String ={
    either match {
      case Right(x) => null
      case Left(x) => x.toString
    }
  }

}
