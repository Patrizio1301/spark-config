package transformation.services

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.DataFrame

/** This class removes columns by
  *
  * @param config drop columns.
  */
object DropColumnsService extends DropColumnsService

/**
  * This trait is an overall service to drop one or several fields.
  */
trait DropColumnsService extends LazyLogging {

  def apply(columnsToDrop: Seq[String], dataframe: DataFrame): DataFrame = {

    logger.info(s"DropColumns: drop columns: ${columnsToDrop.mkString(",")}")
    import utils.implicits.DataFrame._
    dataframe.dropNestedColumn(columnsToDrop: _*)
  }
}
