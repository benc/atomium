package be.vlaanderen.awv.atom.models

import be.vlaanderen.awv.atom.slick.SlickPostgresDriver.simple._
import org.joda.time.LocalDateTime

class EntryTable(tag: Tag, tableName: String) extends Table[EntryModel](tag, tableName) {

  def id = column[Option[Long]]("id", O.AutoInc, O.PrimaryKey)
  def uuid = column[String]("uuid")
  def value = column[String]("value")
  def timestamp = column[LocalDateTime]("timestamp", O.NotNull)

  def * = (id, uuid, value, timestamp) <>(EntryModel.tupled, EntryModel.unapply)

}
