package com.wavesplatform.it.sync

import com.wavesplatform.api.http.alias.CreateAliasV1Request
import com.wavesplatform.api.http.assets.{BurnV1Request, IssueV1Request, MassTransferRequest, ReissueV1Request, SignedTransferV1Request, SponsorFeeRequest, TransferV1Request}
import com.wavesplatform.api.http.leasing.{LeaseCancelV1Request, LeaseV1Request, SignedLeaseCancelV1Request, SignedLeaseV1Request}
import com.wavesplatform.it.api.SyncHttpApi._
import com.wavesplatform.it.api.Transaction
import com.wavesplatform.it.transactions.BaseTransactionSuite
import com.wavesplatform.transaction.lease.{LeaseCancelTransaction, LeaseTransaction}
import com.wavesplatform.transaction.transfer.{MassTransferTransaction, TransferTransaction}
import org.scalatest.CancelAfterFailure
import com.wavesplatform.api.http.DataRequest
import com.wavesplatform.common.state.ByteStr
import com.wavesplatform.state.{BinaryDataEntry, BooleanDataEntry, IntegerDataEntry, StringDataEntry}
import com.wavesplatform.transaction.transfer.MassTransferTransaction.Transfer
import com.wavesplatform.it.util._
import play.api.libs.json.{Json, Writes}

class ObsoleteHandlersSuite extends BaseTransactionSuite with CancelAfterFailure {

  test("alias create") {
    val tx = sender.postJson("/alias/create", CreateAliasV1Request(firstAddress, "testalias", minFee)).asInstanceOf[Transaction].id
    nodes.waitForTransaction(tx)
  }

  test("assets masstransfer"){
    val fee = calcMassTransferFee(2)
    implicit val w: Writes[MassTransferRequest] = Json.writes[MassTransferRequest]
    val transfers = List(Transfer(secondAddress, 1.waves), Transfer(thirdAddress, 2.waves))
    val tx = sender.postJson("/assets/masstransfer", MassTransferRequest(MassTransferTransaction.version, None, firstAddress, transfers, fee, None))
      .asInstanceOf[Transaction].id
    nodes.waitForTransaction(tx)
  }

  test("assets transfer") {
    val tx = sender
      .postJson("/assets/transfer", TransferV1Request(None, None, transferAmount, minFee, firstAddress, None, secondAddress))
      .asInstanceOf[Transaction]
    nodes.waitForTransaction(tx.id)
  }

  test("assets issue, burn, reissue, sponsor") {
    val issue = sender
      .postJson("/assets/issue", IssueV1Request(firstAddress, "testasset", "testasset", someAssetAmount, 2, true, issueFee))
      .asInstanceOf[Transaction]
      .id
    nodes.waitForTransaction(issue)

    val burn = sender.postJson("/assets/burn", BurnV1Request(firstAddress, issue, someAssetAmount / 2 ,minFee)).asInstanceOf[Transaction].id
    nodes.waitForTransaction(burn)

    val reissue = sender.postJson("/assets/reissue", ReissueV1Request(firstAddress, issue, someAssetAmount, true, issueFee)).asInstanceOf[Transaction].id
    nodes.waitForTransaction(reissue)

    val sponsor = sender.postJson("/assets/sponsor", SponsorFeeRequest(1, firstAddress, issue, Some(100L), sponsorFee)).asInstanceOf[Transaction].id
    nodes.waitForTransaction(sponsor)
  }

  test("leasing lease and cancel") {
    val (balance1, eff1) = notMiner.accountBalances(firstAddress)
    val (balance2, eff2) = notMiner.accountBalances(secondAddress)

    val leaseId = sender.postJson("/leasing/lease", LeaseV1Request(firstAddress, leasingAmount, minFee, secondAddress)).asInstanceOf[Transaction].id
    nodes.waitForTransaction(leaseId)

    notMiner.assertBalances(firstAddress, balance1 - minFee, eff1 - leasingAmount - minFee)
    notMiner.assertBalances(secondAddress, balance2, eff2 + leasingAmount)

    val leaseCancel = sender.postJson("/leasing/cancel", LeaseCancelV1Request(firstAddress, leaseId, minFee)).asInstanceOf[Transaction].id
    nodes.waitForTransaction(leaseCancel)

    notMiner.assertBalances(firstAddress, balance1 - 2 * minFee, eff1 - 2 * minFee)
    notMiner.assertBalances(secondAddress, balance2, eff2)
  }

  test("assets broadcast transfer") {
    val json = Json.obj(
      "type"      -> TransferTransaction.typeId,
      "sender"    -> firstAddress,
      "recipient" -> secondAddress,
      "fee"       -> minFee,
      "amount"    -> transferAmount
    )

    val signedRequestResponse = sender.postJsonWithApiKey(s"/transactions/sign/$thirdAddress", json)
    val transfer              = Json.parse(signedRequestResponse.getResponseBody).as[SignedTransferV1Request]

    val transferId = sender.postJson("/assets/broadcast/transfer", transfer).asInstanceOf[Transaction].id
    nodes.waitForTransaction(transferId)
  }

  test("leasing broadcast lease and cancel") {
    val jsonL = Json.obj(
      "type"      -> LeaseTransaction.typeId,
      "sender"    -> firstAddress,
      "recipient" -> secondAddress,
      "fee"       -> minFee,
      "amount"    -> transferAmount
    )

    val r1    = sender.postJsonWithApiKey(s"/transactions/sign/$thirdAddress", jsonL)
    val lease = Json.parse(r1.getResponseBody).as[SignedLeaseV1Request]

    val leaseId = sender.postJson("/leasing/broadcast/lease", lease).asInstanceOf[Transaction].id
    nodes.waitForTransaction(leaseId)

    val jsonLC = Json.obj(
      "type"    -> LeaseCancelTransaction.typeId,
      "sender"  -> firstAddress,
      "fee"     -> minFee,
      "leaseId" -> leaseId
    )

    val r2     = sender.postJsonWithApiKey(s"/transactions/sign/$thirdAddress", jsonLC)
    val leaseC = Json.parse(r2.getResponseBody).as[SignedLeaseCancelV1Request]

    val leaseCId = sender.postJson("/leasing/broadcast/cancel", leaseC).asInstanceOf[Transaction].id
    nodes.waitForTransaction(leaseCId)
  }

  test("addresses data") {
    implicit val w: Writes[DataRequest] = Json.writes[DataRequest]
    val data = List(
      IntegerDataEntry("int", 923275292849183L),
      BooleanDataEntry("bool", value = true),
      BinaryDataEntry("blob", ByteStr(Array.tabulate(445)(_.toByte))),
      StringDataEntry("str", "AAA-AAA")
    )
    val fee = calcDataFee(data)
    val tx  = sender.postJson("/addresses/data", DataRequest(1, firstAddress, data, fee)).asInstanceOf[Transaction].id
    nodes.waitForTransaction(tx)
  }

}
