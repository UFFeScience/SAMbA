package br.uff.spark.schema

/**
  * <p>
  * Use this schema when the result has only one line.
  * So, just implement the follow methods:
  * override def splitData(value: String): Array[String]
  * override def geFieldsNames: Array[String]
  * </p>
  *
  * For instance:
  *
  * public class SampleSingleLineResult implements SingleLineSchema<String>{
  *
  *    @Override
  *    public String[] getFieldsNames() {
  *        return new String[]{"Sample Field Name"};
  *    }
  *
  *    @Override
  *    public String[] splitData(String value) {
  *        return new String[]{value};
  *    }
  *
  * }
  *
  * @tparam T type of value
  */
trait SingleLineSchema[T] extends DataElementSchema[T] {

  override def getSplittedData(value: T) = Array(splitData(value))

  def splitData(value: T): Array[String]

}
