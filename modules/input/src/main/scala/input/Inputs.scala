package input

import input.inputs.TFRecordsImp.TFRecordInstance
import input.inputs.CsvImp.CsvInstance
import scala.input.inputs.ExcelImp.ExcelInstance

object Inputs extends TFRecordInstance with CsvInstance with ExcelInstance