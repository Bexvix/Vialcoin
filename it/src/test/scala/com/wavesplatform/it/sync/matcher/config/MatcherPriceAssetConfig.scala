package com.wavesplatform.it.sync.matcher.config

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory.{empty, parseString}
import com.wavesplatform.account.PrivateKeyAccount

import com.wavesplatform.it.NodeConfigs.Default
import com.wavesplatform.it.sync.CustomFeeTransactionSuite.defaultAssetQuantity
import com.wavesplatform.it.sync.matcher.config.MatcherDefaultConfig._
import com.wavesplatform.it.util._
import com.wavesplatform.transaction.assets.IssueTransactionV1
import com.wavesplatform.transaction.assets.exchange.AssetPair

import scala.util.Random

object MatcherPriceAssetConfig {

  private val _Configs: Seq[Config] = (Default.last +: Random.shuffle(Default.init).take(3))
    .zip(Seq(matcherConfig.withFallback(minerDisabled), minerDisabled, minerDisabled, empty()))
    .map { case (n, o) => o.withFallback(n) }

  private val aliceSeed = _Configs(1).getString("account-seed")
  private val bobSeed   = _Configs(2).getString("account-seed")
  private val alicePk   = PrivateKeyAccount.fromSeed(aliceSeed).right.get
  private val bobPk     = PrivateKeyAccount.fromSeed(bobSeed).right.get

  val Decimals: Byte = 2

  val usdAssetName = "USD-X"
  val wctAssetName = "WCT-X"
  val ethAssetName = "ETH-X"

  val IssueUsdTx: IssueTransactionV1 = IssueTransactionV1
    .selfSigned(
      sender = alicePk,
      name = usdAssetName.getBytes(),
      description = "asset description".getBytes(),
      quantity = defaultAssetQuantity,
      decimals = Decimals,
      reissuable = false,
      fee = 1.waves,
      timestamp = System.currentTimeMillis()
    )
    .right
    .get

  val IssueWctTx: IssueTransactionV1 = IssueTransactionV1
    .selfSigned(
      sender = bobPk,
      name = wctAssetName.getBytes(),
      description = "asset description".getBytes(),
      quantity = defaultAssetQuantity,
      decimals = Decimals,
      reissuable = false,
      fee = 1.waves,
      timestamp = System.currentTimeMillis()
    )
    .right
    .get

  val IssueEthTx: IssueTransactionV1 = IssueTransactionV1
    .selfSigned(
      sender = alicePk,
      name = ethAssetName.getBytes(),
      description = "asset description".getBytes(),
      quantity = defaultAssetQuantity,
      decimals = 8,
      reissuable = false,
      fee = 1.waves,
      timestamp = System.currentTimeMillis()
    )
    .right
    .get

  val IssueBtcTx: IssueTransactionV1 = IssueTransactionV1
    .selfSigned(
      sender = bobPk,
      name = "BTC-X".getBytes(),
      description = "asset description".getBytes(),
      quantity = defaultAssetQuantity,
      decimals = 8,
      reissuable = false,
      fee = 1.waves,
      timestamp = System.currentTimeMillis()
    )
    .right
    .get

  val BtcId = IssueBtcTx.id()
  val EthId = IssueEthTx.id()
  val UsdId = IssueUsdTx.id()
  val WctId = IssueWctTx.id()

  val wctUsdPair = AssetPair(
    amountAsset = Some(WctId),
    priceAsset = Some(UsdId)
  )

  val wctWavesPair = AssetPair(
    amountAsset = Some(WctId),
    priceAsset = None
  )

  val ethWavesPair = AssetPair(
    amountAsset = Some(EthId),
    priceAsset = None
  )

  val ethBtcPair = AssetPair(
    amountAsset = Some(EthId),
    priceAsset = Some(BtcId)
  )

  val wavesUsdPair = AssetPair(
    amountAsset = None,
    priceAsset = Some(UsdId)
  )

  val ethUsdPair = AssetPair(
    amountAsset = Some(EthId),
    priceAsset = Some(UsdId)
  )

  val orderLimit = 10

  private val updatedMatcherConfig = parseString(s"""waves.matcher {
                                                    |  price-assets = [ "$UsdId", "$BtcId", "WAVES" ]
                                                    |  rest-order-limit = $orderLimit
                                                    |}""".stripMargin)

  val Configs: Seq[Config] = _Configs.map(updatedMatcherConfig.withFallback(_))

}
