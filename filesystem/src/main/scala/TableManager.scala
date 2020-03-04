import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.io.Tcp.Message

object TableManager {

  private case class Table(data: Map[String, String] = Map()) {
    def +(entry: (String, String)) = this.copy(data = this.data + entry)

    def get(key: String) = this.data.get(key)
  }

  def apply(): Behavior[FileSystemBackend.TableCommand] = handleCommand(Table())

  def handleCommand(table: Table): Behavior[FileSystemBackend.TableCommand] =
    Behaviors.receiveMessage {
      case message: FileSystemBackend.Insert => this.handleInsert(table, message)
      case message: FileSystemBackend.Select => this.handleSelect(table, message)
    }

  def handleInsert(table: Table, message: FileSystemBackend.Insert): Behavior[FileSystemBackend.TableCommand] = {
    val newTable = table + (message.key -> message.value)
    message.replyTo ! FileSystemBackend.InsertSuccessful(message.tableName, message.key, message.value)
    this.handleCommand(newTable)
  }

  def handleSelect(table: Table, message: FileSystemBackend.Select): Behavior[FileSystemBackend.TableCommand] = {
    table.get(message.key).fold(
      this.responseWithKeyError(message)
    )(
      this.respondWithValue(message, _)
    )
    Behaviors.same
  }

  def responseWithKeyError(message: FileSystemBackend.Select) =
    message.replyTo ! FileSystemBackend.KeyError(message.tableName, message.key)

  def respondWithValue(message: FileSystemBackend.Select, value: String) =
    message.replyTo ! FileSystemBackend.SelectSuccessful(message.tableName, message.key, value)
}
