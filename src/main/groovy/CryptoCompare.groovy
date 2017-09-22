import groovy.json.JsonOutput
import groovy.json.JsonSlurper

/**
 * Created by kindler on 13/09/2017.
 */
class CryptoCompare {

    def getHistData(cryptoCurrency, inCurrency, frequency="day", since=0, exchange="Kraken") {
        def cache = new File(System.getProperty('java.io.tmpdir'), "cache.${frequency}.${cryptoCurrency}.${inCurrency}.${exchange}.cryptocompare")
        def response = ""

        // cache file for 8 hours
        if (cache.exists() && cache.lastModified() + 1000 * 60 * 8 >= System.currentTimeMillis()) {
            response = cache.text
        } else {
            def url = "https://min-api.cryptocompare.com/data/histo$frequency?fsym=$cryptoCurrency&tsym=$inCurrency&e=$exchange&extraParams=Test&allData=true"
            response = new URL(url).getText()
            cache.text = response
        }

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
