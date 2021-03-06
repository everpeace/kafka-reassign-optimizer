package com.github.everpeace.kafka.reassign_optimizer.util

// borrowed from https://stackoverflow.com/questions/7539831/scala-draw-table-to-console
// and customized.
object Tabulator {
  val rowSeparatorString = ""
  val rowSeparatorColumnSeparatorString = ""
  val columnSeparatorString = "   "

  def format(table: Seq[Seq[Any]], legends: Seq[(Char, String)] = Seq.empty, numRowHeader: Int = 1, numRowFooter: Int = 1): String = table match {
    case Seq() => ""
    case _ =>
      val sizes = for (row <- table) yield {
        for (cell <- row) yield if (cell == null) 0 else cell.toString.length
      }
      val colSizes = for (col <- sizes.transpose) yield col.max
      val rows = for (row <- table) yield formatRow(row, colSizes)

      val formattedLegends = for {
        (c, des) <- legends
      } yield {
        List(s"%${colSizes.head}s".format(c), s"%-${colSizes.tail.sum}s".format(des))
          .mkString(columnSeparatorString, columnSeparatorString, columnSeparatorString)
      }
      val (headers, remained) = rows.splitAt(numRowHeader)
      val (footersR, contentsR) = remained.reverse.splitAt(numRowFooter)
      formatRows(rowSeparator(colSizes), headers, contentsR.reverse, footersR.reverse, formattedLegends)
  }

  def formatRows(rowSeparator: String, headers: Seq[String], contents: Seq[String], footers: Seq[String], legends: Seq[String]): String = {
    (rowSeparator ::
      headers.toList :::
      rowSeparator ::
      contents.toList :::
      rowSeparator ::
      footers.toList :::
      legends.toList :::
      List()).mkString("\n")
  }

  def formatRow(row: Seq[Any], colSizes: Seq[Int]): String = {
    val cells = for ((item, size) <- row.zip(colSizes)) yield {
      if (size == 0) "" else ("%" + size + "s").format(item)
    }
    cells.mkString(columnSeparatorString, columnSeparatorString, columnSeparatorString)
  }

  def rowSeparator(colSizes: Seq[Int]): String = colSizes map {
    rowSeparatorString * _
  } mkString(rowSeparatorColumnSeparatorString, rowSeparatorColumnSeparatorString, rowSeparatorColumnSeparatorString)
}
