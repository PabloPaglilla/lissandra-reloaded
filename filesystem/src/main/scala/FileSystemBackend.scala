import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}

object FileSystemBackend {

  sealed trait StorageCommand

  final case class CreateTable(tableName: String, replyTo: ActorRef[StorageResponse]) extends StorageCommand

  final case class Insert(
                           tableName: String,
                           key: String,
                           value: String, replyTo: ActorRef[StorageResponse]
                         ) extends StorageCommand

  final case class Select(
                           tableName: String,
                           key: String,
                           replyTo: ActorRef[StorageResponse]
                         ) extends StorageCommand

  sealed trait StorageResponse

  sealed trait StorageSuccess extends StorageResponse

  sealed trait StorageError extends StorageResponse

  final case class TableCreated(tableName: String) extends StorageSuccess

  final case class InsertSuccessful(tableName: String, key: String, value: String) extends StorageSuccess

  final case class SelectSuccessful(tableName: String, key: String, value: String) extends StorageSuccess

  final case class TableAlreadyExists(tableName: String) extends StorageError

  final case class TableDoesNosExist(tableName: String) extends StorageError

  final case class KeyError(tableName: String, key: String) extends StorageError

  private case class Table(data: Map[String, String] = Map()) {
    def +(entry: (String, String)) = this.copy(data = this.data + entry)

    def get(key: String) = this.data.get(key)
  }

  def apply(): Behavior[StorageCommand] = handleCommand(Map())

  def handleCommand(database: Map[String, Table]): Behavior[StorageCommand] =
    Behaviors.receiveMessage {
      case message@CreateTable(_, _) => this.handleCreate(database, message)
      case message@Insert(_, _, _, _) => this.handleInsert(database, message)
      case message@Select(_, _, _) => this.handleSelect(database, message)
    }

  def handleCreate(database: Map[String, Table], message: CreateTable): Behavior[StorageCommand] =
    database.get(message.tableName).fold {
      val newDatabase = database + (message.tableName -> Table())
      message.replyTo ! TableCreated(message.tableName)
      this.handleCommand(newDatabase)
    } { _ =>
      message.replyTo ! TableAlreadyExists(message.tableName)
      Behaviors.same
    }

  def handleInsert(database: Map[String, Table], message: Insert): Behavior[StorageCommand] =
    database.get(message.tableName).fold[Behavior[StorageCommand]] {
      message.replyTo ! TableDoesNosExist(message.tableName)
      Behaviors.same
    } { table =>
      val newTable = table + (message.key -> message.value)
      val newDatabase = database.updated(message.tableName, newTable)
      message.replyTo ! InsertSuccessful(message.tableName, message.key, message.value)
      this.handleCommand(newDatabase)
    }

  def handleSelect(database: Map[String, Table], message: Select): Behavior[StorageCommand] = {
    database.get(message.tableName).fold {
      message.replyTo ! TableDoesNosExist(message.tableName)
    } {
      table => this.getValueFromTable(table, message)
    }
    Behaviors.same
  }

  def getValueFromTable(table: Table, message: Select) =
    table.get(message.key).fold {
      message.replyTo ! KeyError(message.tableName, message.key)
    } {
      value => message.replyTo ! SelectSuccessful(message.tableName, message.key, value)
    }
}
