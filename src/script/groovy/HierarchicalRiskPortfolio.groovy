#!/usr/bin/env groovy


import kic.dataframe.*

import static java.lang.Math.*

//@Grab(group='com.apporiented', module='hierarchical-clustering', version='1.1.1-SNAPSHOT')
//@Grab(group='kic', module='dataframe', version='1.0-SNAPSHOT')
import kic.dataframe.DataFrame
import static kic.dataframe.Operations.*

//@Grab(group='org.apache.commons', module='commons-math3', version='3.6.1')
import org.apache.commons.math3.stat.correlation.*

// runtime parameters
currencies = ["BTC", "LTC", "ETH", "XRP", "XMR", "DASH", "REP", "ZEC", "ETC"]
referenceCurrency = "EUR"
exchange="CCCAGG"
frequency = "day"




// predefined operations
calcReturn = deltaOperation({ sell, buy -> sell / buy - 1d})
calcLogReturn = deltaOperation({ sell, buy -> log(sell / buy)})

// business logic
DataFrame prices = getPriceDataFrame(currencies)

println(prices)

returns = prices.slide(2, calcLogReturn)
println(returns)

objective = returns.slide(60, covarianceCorrelationEstimator(3, false))
println(objective)

weights = objective.slide(1, optimizePortfolio()) // we could do a weely rebalancing
println(weights)

// Functions
DataFrame getPriceDataFrame(currencies) {
    CryptoCompare cc = new CryptoCompare()
    DataFrame prices = new DataFrame()

    currencies.forEach { ccy ->
        cc.getHistData(ccy, referenceCurrency, frequency, 0,  exchange).forEach { bar ->
            prices.upsert(bar.time, ccy, bar.close)
        }
    }

    return prices
}


def covarianceCorrelationEstimator(int minSize, boolean demean = false) {
    return { DataFrame window, DataFrame result ->
        // first we need the intersect of the first and the last row
        returns = window.select(window.getFirstRowAndLastRowsColumnsIntersect())
        if (returns.getColumnOrder().size() < minSize) return

        // then we calculate the covariance and the correlationmatrix
        returnsArray = returns.asDoubleMatrix({ d -> d } , false)
        cov = new Covariance(returnsArray, !demean)
        corr = new PearsonsCorrelation(cov)

        // then we convet the matrices into dataframes and store them in the result
        labels = returns.getColumnOrder()
        covDat = cov.getCovarianceMatrix().getData()
        corrDat = corr.getCorrelationMatrix().getData()

        DataFrame covarianceMatrix = new DataFrame()
        DataFrame correlationMatrix = new DataFrame()
        for (int i = 0; i < labels.size(); i++) {
            for (int j = 0; j < labels.size(); j++) {
                covarianceMatrix.upsert(labels.get(i), labels.get(j), covDat[i][j])
                correlationMatrix.upsert(labels.get(i), labels.get(j), corrDat[i][j])
            }
        }

        clustered = correlationMatrix.clusterSymetric({d -> sqrt(1d - d / 2d) })
        result.upsert(window.getLastRow().rowKey, "covariance", covarianceMatrix)
        result.upsert(window.getLastRow().rowKey, "correlation", correlationMatrix)
        result.upsert(window.getLastRow().rowKey, "clustered", clustered)

    }
}



def optimizePortfolio() {
    return { DataFrame window, DataFrame result ->
        DataFrame.Tuple lastRow = window.getLastRow()
        DataFrame covariance = lastRow.value["covariance"]
        DataFrame clusteredCorrelation = lastRow.value["clustered"]
        cItems = [clusteredCorrelation.getColumnOrder()]
        weights = clusteredCorrelation.getColumnOrder().collectEntries { [(it): 1d] }

        while (cItems.size() > 0) {
            cItems = cItems.findAll { it.size() > 1 }
                           .collectMany { [it.subList(0, it.size().intdiv(2)), it.subList(it.size().intdiv(2), it.size())] }

            for (int i = 0; i < cItems.size(); i += 2) {
                cItems0 = cItems[i]
                cItems1 = cItems[i + 1]
                cVar0 = inverseVariancePortfolio(covariance.select(cItems0, cItems0))
                cVar1 = inverseVariancePortfolio(covariance.select(cItems1, cItems1))
                alpha = 1 - cVar0 / (cVar0 + cVar1)
                cItems0.each { weights[it] *= alpha }
                cItems1.each { weights[it] *= 1 - alpha }
                // println("$cItems0 $cVar0 / $cItems1 $cVar1 / $alpha")
            }
        }

        weights.each { result.upsert(lastRow.rowKey, it.key, it.value) }
    }
}

def inverseVariancePortfolio(DataFrame cov) {
    return Linalg.calcScalar(cov, { d -> d }, { mx ->
        inverseDiagonal = mx.diagonal.map({ d -> 1d / d })
        sum = inverseDiagonal.sum()
        w = inverseDiagonal.map({ d -> d / sum })
        var = w.matrix.multiply(mx.matrix).multiply(w.matrix.transpose()).getEntry(0,0)
        return var;
    })
}

/*finally compute the hrp portfolio
def getIVP(cov,**kargs):
    # Compute the inverse-variance portfolio
    ivp=1./np.diag(cov)
    ivp/=ivp.sum()
    return ivp

def getClusterVar(cov,cItems):
    # Compute variance per cluster
    cov_=cov.loc[cItems,cItems] # matrix slice
    w_=getIVP(cov_).reshape(-1,1)
    cVar=np.dot(np.dot(w_.T,cov_),w_)[0,0]
    return cVar

def getRecBipart(cov,sortIx):
    # Compute HRP alloc
    w=pd.Series(1,index=sortIx)
    cItems=[sortIx] # initialize all items in one cluster
    while len(cItems)>0:
        cItems=[i[j:k] for i in cItems for j,k in ((0,len(i)/2), (len(i)/2,len(i))) if len(i)>1] # bi-section
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
