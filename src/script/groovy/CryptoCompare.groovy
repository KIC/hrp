import groovy.json.JsonOutput
import groovy.json.JsonSlurper

/**
 * Created by kindler on 13/09/2017.
 */
class CryptoCompare {
    def getHistData(cryptoCurrency, inCurrency, frequency="day", since=0, exchange="Kraken") {
        def url = "https://min-api.cryptocompare.com/data/histo$frequency?fsym=$cryptoCurrency&tsym=$inCurrency&e=$exchange&extraParams=Test&allData=true"
        def response = new URL(url).getText()

        return fromJson(response).Data
                .findAll { it.time > since }
                .collect {
            it << ["cryptoCurrency" : cryptoCurrency]
            it << ["inCurrency" : inCurrency]
            it << ["exchange" : exchange]
            it
        }
    }

    def fromJson(String jsonText) {
        return new JsonSlurper().parseText(jsonText)
    }

}
