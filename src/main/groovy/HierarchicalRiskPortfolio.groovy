#!/usr/bin/env groovy
import groovy.json.JsonOutput
import groovy.json.JsonSlurper

import java.util.function.BiConsumer
import static kic.dataframe.Operations.*
import static java.lang.Math.*

// @Grab(group='kic', module='dataframe', version='1.0-SNAPSHOT')
import kic.dataframe.DataFrame

@Grab(group='org.apache.commons', module='commons-math3', version='3.6.1')
import org.apache.commons.math3.stat.correlation.*

// runtime parameters
currencies = ["BTC", "LTC", "ETH", "XRP", "XMR", "DASH", "REP", "ZEC", "ETC"]
referenceCurrency = "EUR"
exchange="CCCAGG"
frequency = "day"

// predefined operations
calcReturn = deltaOperation({ sell, buy -> sell / buy - 1d})
calcLogReturn = deltaOperation({ sell, buy -> log(sell / buy)})
calcCovCorr = getCovarianceCorrelationEstimator(3, false)

// business logic
DataFrame prices = getPriceDataFrame(currencies)

println(prices)

returns = prices.slide(2, calcLogReturn)
println(returns)

objective = returns.slide(60, calcCovCorr)
println(objective)

weights = null // objective.slide(1, hrPortfolio) // we could do a weely rebalancing

// Functions
DataFrame getPriceDataFrame(currencies) {
    DataFrame prices = new DataFrame()

    currencies.forEach { ccy ->
        getHistData(ccy, referenceCurrency, frequency, 0,  exchange).forEach { bar ->
            prices.upsert(bar.time, ccy, bar.close)
        }
    }

    return prices
}

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

def fromJson(jsonText) {
    return new JsonSlurper().parseText(jsonText)
}

def toJson(object) {
    return JsonOutput.toJson(object)
}

def getCovarianceCorrelationEstimator(minSize, demean = false) {
    return { DataFrame window, DataFrame result ->
        // first we need the intersect of the first and the last row
        returns = window.select(window.getFirstRowAndLastRowsColumnsIntersect())
        if (returns.getColumnOrder().size() < minSize) return

        // then we calculate the covariance and the correlationmatrix
        returnsArray = returns.asDoubleMatrix({ d -> return (double) d }, false)
        cov = new Covariance(returnsArray, !demean)
        corr = new PearsonsCorrelation(cov)

        // finally we convet the matrices into dataframes and store them in the result
        labels = returns.getColumnOrder()
        covDat = cov.getCovarianceMatrix().getData()
        corrDat = corr.getCorrelationMatrix().getData()

        // println("${covDat.length} ${corrDat.length}")

        DataFrame covarianceMatrix = new DataFrame()
        DataFrame correlationMatrix = new DataFrame()
        for (int i = 0; i < labels.size(); i++) {
            for (int j = 0; j < labels.size(); j++) {
                covarianceMatrix.upsert(labels.get(i), labels.get(j), covDat[i][j])
                correlationMatrix.upsert(labels.get(i), labels.get(j), corrDat[i][j])
            }
        }

        clustered = correlationMatrix.clusterSymetric({d -> return sqrt(1d - d / 2d) })
        result.upsert(window.getLastRow().rowKey, "covariance", covarianceMatrix)
        result.upsert(window.getLastRow().rowKey, "correlation", correlationMatrix)
        result.upsert(window.getLastRow().rowKey, "clustered", clustered)

    }
}


/*finally compute the hrp portfolio
def getIVP(cov,**kargs):
    # Compute the inverse-variance portfolio
    ivp=1./np.diag(cov)
    ivp/=ivp.sum()
    return ivp

def getRecBipart(cov,sortIx):
    # Compute HRP alloc
    w=pd.Series(1,index=sortIx)
    cItems=[sortIx] # initialize all items in one cluster
    while len(cItems)>0:
        cItems=[i[j:k] for i in cItems for j,k in ((0,len(i)/2), \
           (len(i)/2,len(i))) if len(i)>1] # bi-section
        for i in xrange(0,len(cItems),2): # parse in pairs
            cItems0=cItems[i] # cluster 1
            cItems1=cItems[i+1] # cluster 2
            cVar0=getClusterVar(cov,cItems0)
            cVar1=getClusterVar(cov,cItems1)
            alpha=1-cVar0/(cVar0+cVar1)
            w[cItems0]*=alpha # weight 1
            w[cItems1]*=1-alpha # weight 2
    return w
 */