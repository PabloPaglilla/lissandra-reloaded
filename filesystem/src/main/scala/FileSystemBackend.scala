import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}

object FileSystemBackend {

  sealed trait StorageCommand {
    def tableName: String
    def replyTo: ActorRef[StorageResponse]
  }

  sealed trait TableCommand extends StorageCommand

  final case class CreateTable(
                                tableName: String,
                                consistency: TableConsistency,
                                replyTo: ActorRef[StorageResponse]
                              ) extends StorageCommand

  final case class Insert(
                           tableName: String,
                           key: String,
                           value: String, replyTo: ActorRef[StorageResponse]
                         ) extends TableCommand

  final case class Select(
                           tableName: String,
                           key: String,
                           replyTo: ActorRef[StorageResponse]
                         ) extends TableCommand

  final case class DeleteTable(tableName: String, replyTo: ActorRef[StorageResponse]) extends TableCommand

  final case class GetConsistency(tableName: String, replyTo: ActorRef[StorageResponse]) extends TableCommand

  sealed trait StorageResponse

  sealed trait StorageSuccess extends StorageResponse

  sealed trait StorageError extends StorageResponse

  final case class TableCreated(tableName: String) extends StorageSuccess

  final case class InsertSuccessful(tableName: String, key: String, value: String) extends StorageSuccess

  final case class SelectSuccessful(tableName: String, key: String, value: String) extends StorageSuccess

  final case class TableDeleted(tableName: String) extends StorageSuccess

  final case class TableConsistencyResponse(tableName: String, consistency: TableConsistency) extends StorageSuccess

  final case class TableAlreadyExists(tableName: String) extends StorageError

  final case class TableDoesNosExist(tableName: String) extends StorageError

  final case class KeyError(tableName: String, key: String) extends StorageError

  def apply(): Behavior[StorageCommand] = handleCommand(Map())

  def handleCommand(database: Map[String, ActorRef[TableCommand]]): Behavior[StorageCommand] =
    Behaviors.receive { (context, message) =>
      message match {
        case message: CreateTable => this.handleCreate(database, context, message)
        case message: DeleteTable => this.handleDelete(database, message)
        case message: TableCommand => this.handleTableCommand(database, message)
      }
    }

  def handleCreate(
                    database: Map[String, ActorRef[TableCommand]],
                    context: ActorContext[StorageCommand],
                    message: CreateTable): Behavior[StorageCommand] =
    database.get(message.tableName).fold(
      this.createTable(database, context, message)
    )(
      _ => this.respondWithTableAlreadyExists(message)
    )

  def createTable(
                   database: Map[String, ActorRef[TableCommand]],
                   context: ActorContext[StorageCommand],
                   message: CreateTable): Behavior[StorageCommand] = {
    val tableManager = context.spawn(TableManager(), message.tableName)
    val newDatabase = database + (message.tableName -> tableManager)
    message.replyTo ! TableCreated(message.tableName)
    this.handleCommand(newDatabase)
  }

  def handleDelete(
                  database: Map[String, ActorRef[TableCommand]],
                  message: DeleteTable
                  ): Behavior[StorageCommand] =
    database.get(message.tableName).fold[Behavior[StorageCommand]] {
      message.replyTo ! TableDoesNosExist(message.tableName)
      Behaviors.same
    } {
      this.deleteTable(database, _, message)
    }

  def deleteTable(
                   database: Map[String, ActorRef[TableCommand]],
                   tableManager: ActorRef[TableCommand],
                   message: DeleteTable): Behavior[StorageCommand] = {
    val newDatabase = database - message.tableName
    tableManager ! message
    this.handleCommand(newDatabase)
  }

  def respondWithTableAlreadyExists(message: CreateTable): Behavior[StorageCommand] = {
    message.replyTo ! TableAlreadyExists(message.tableName)
    Behaviors.same
  }

  def handleTableCommand(database: Map[String, ActorRef[TableCommand]], message: TableCommand): Behavior[StorageCommand] = {
    database.get(message.tableName).fold {
      message.replyTo ! TableDoesNosExist(message.tableName)
    } {
      tableManager => tableManager ! message
    }

    Behaviors.same
  }

}
