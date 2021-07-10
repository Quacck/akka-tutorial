package de.hpi.spark_tutorial

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{asc, desc}

object Sindy {

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {

    // Importing implicit encoders for standard library classes and tuples that are used as Dataset types
    import spark.implicits._

    var allCells = spark.emptyDataset[(String, String)]

    inputs.foreach(fileName => {
      val dataframe = spark.read
        .option("inferSchema", "true")
        .option("header", "true")
        .option("delimiter", ";")
        .csv(fileName)
        .toDF()
      val columns = dataframe.columns
      val newCells = dataframe.flatMap(row => columns.map(
        columnname => (row.get(row.fieldIndex(columnname)).toString(), columnname)
      ))
      allCells = allCells.union(newCells)
    })
    val inclusionLists = allCells.groupByKey(cell => cell._1)
      //group by value and unite columnNames to set
      .mapGroups((key, iterator) => iterator.map(cell => cell._2).toList.distinct)
      //create inclusion lists with dependant, referenced columns
      .flatMap(attributeSet => {
          val references = attributeSet.map(dependant => (dependant, attributeSet.filter(reference => reference != dependant)))
          //filter out empty reference lists
        references
        })
    val result = inclusionLists
      .groupByKey(inclusionList => inclusionList._1)
      .mapGroups((key, inclusionList) => {
        var intersection = inclusionList.next()._2
        while (inclusionList.hasNext){
          intersection = intersection.intersect(inclusionList.next()._2)
        }
        (key, intersection)
      }).filter(row => row._2.nonEmpty)
      .sort(asc("_1"))

    result.collectAsList().forEach(tuple => println(tuple._1 + " < " + tuple._2.mkString(", ")))
//    * --outer join all datasets--
//    *   each node:
//    *     - reads local share of data
//    *     - split input records into cells --> {value, columnName}, ...
//    *   place same-value-cells on same node with partitioning function p
//    *     (e.g. p(value) = hash(value) % n)
//    *   each node:
//    *     - group by value and unite columnNames to set
//    *     - ditch the value --> only set of column names in new entry
//    * --create inclusion lists--
//    *     - create inclusion lists for every columnName set S: (a, S\a) with a in S
//    *   globally group by first attribute/columnName a
//    *     and intersect columnName sets (Schnittmenge nehmen)

  }
}
