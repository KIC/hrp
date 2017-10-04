import kic.dataframe.*
import kic.dataframe.join.*

currencies = ["ETH", "XMR"]
referenceCurrency = "EUR"
exchange="CCCAGG"
frequency = "day"

DataFrame prices = getPriceDataFrame(currencies)
Join join = new Join(prices, "prices_A", {l -> l.select(["ETH"])})
def jonedDF = join.left(prices, "proces_B", new OuterJoinFillLastRow( { l, r -> r.select(l.getColumnOrder())} ))
                  .left(prices, "proces_C", new OuterJoinFillLastRow( { l, r -> r.select(["XMR"])} ))

println(jonedDF)

DataFrame getPriceDataFrame(currencies) {
    CryptoCompare cc = new CryptoCompare()
    DataFrame prices = new DataFrame()

    currencies.forEach { ccy ->
        cc.getHistData(ccy, referenceCurrency, frequency, 0, exchange).forEach { bar ->
            prices.upsert(bar.time, ccy, bar.close)
        }
    }

    return prices
}