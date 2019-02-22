package ru.itclover.tsp.services
import org.tarantool.TarantoolClientImpl

import scala.util.Try

object TarantoolService {
  case class TarantoolConf(
    host: String,
    port: Int,
    dbName: String,
    userName: Option[String] = None,
    password: Option[String] = None,
    timeoutSec: Long
  )

  def fetchFieldsTypesInfo(query: String, conf: TarantoolConf): Try[Seq[(Symbol, Class[_])]] = {
    val provider = socketChannelProvider(conf)
    val config = ???
    val client = new TarantoolClientImpl(provider, config)
    val result = client.sqlSyncOps.query(s"SELECT * FROM ($query) LIMIT 1")
  }

  import org.tarantool.SocketChannelProvider
  import java.io.IOException
  import java.net.InetSocketAddress
  import java.nio.channels.SocketChannel

  def socketChannelProvider(conf: TarantoolConf): SocketChannelProvider = (retryNumber: Int, lastError: Throwable) => {
    if (lastError != null) lastError.printStackTrace(System.out)
    try SocketChannel.open(new InetSocketAddress(conf.host, conf.port))
    catch {
      case e: IOException =>
        throw new IllegalStateException(e)
    }
  }
}
