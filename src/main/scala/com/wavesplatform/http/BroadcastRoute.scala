package com.wavesplatform.http

import com.wavesplatform.api.http.ApiError
import com.wavesplatform.network._
import com.wavesplatform.transaction.{Transaction, ValidationError}
import com.wavesplatform.utils.Execution
import com.wavesplatform.utx.UtxPool
import io.netty.channel.group.ChannelGroup

import scala.concurrent.Future

trait BroadcastRoute {
  def utx: UtxPool

  def allChannels: ChannelGroup

  implicit val scheduler = Execution.globalEC

  protected def doBroadcast(v: Either[ValidationError, Transaction]): Future[Either[ApiError, Transaction]] = Future {
    val r = for {
      tx <- v
      r  <- utx.putIfNew(tx)
    } yield {
      val (added, _) = r
      if (added) allChannels.broadcastTx(tx, None)
      tx
    }

    r.left.map(ApiError.fromValidationError)
  }
}
