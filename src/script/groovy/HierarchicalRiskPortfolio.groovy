#!/usr/bin/env groovy
import groovy.json.JsonOutput
import static java.lang.Math.*

//@Grab(group='kic', module='dataframe', version='1.0-SNAPSHOT', changing=true)
import kic.dataframe.*
import kic.dataframe.linalg.*
import kic.pipeline.PipelineExecutor
import static kic.dataframe.Operations.*

// TODO Next
// TODO make rebalancing happening on sundays
// TODO add trading costs
// TODO add minimum variance portfolio (markowiz) as 2nd benchmark
// TODO we could also plot a video of the clustered correlation matrix like so:
// http://www.gnuplotting.org/animation-video/ || http://www.gnuplotting.org/tag/animation/ && http://gnuplot.sourceforge.net/demo_svg/heatmaps.html


// runtime parameters - later parse this from commandline: http://mrhaki.blogspot.ch/2009/09/groovy-goodness-parsing-commandline.html
currencies = ["BTC", "LTC", "ETH", "XRP", "XMR", "DASH", "REP", "ZEC", "ETC"]
referenceCurrency = "EUR"
exchange="CCCAGG"
frequency = "day"
covariance_max_0_percent = 10
trading_costs = 0.0016 // 0.16% on kraken

// constants
PATH = new File(getClass().protectionDomain.codeSource.location.path).getParentFile()
PLOTPATH = new File(PATH, "../gnuplot/")
COVARIANCE = "covariance"
CORRELATION = "correlation"
CLUSTERED = "clustered"
VaR95_Z_SCORE = 1.645 //Stddev to VaR factor = 1.645

println("running at: $PATH\nusing temp: ${System.getProperty('java.io.tmpdir')}")

// NOTE we have weired huge jumps i.e. dash jumps 1400% in one day, so we ignore 3 fold and bigger jumps in prices
def experiment = new PipelineExecutor("prices", getPriceDataFrame(currencies))
    .then("returns", { it.slide(2, deltaOperation({ sell, buy -> sell != buy && sell < 3 * buy ? sell / buy - 1d : null }))})
    .then("objective", { it.slide(60, covarianceCorrelationEstimator(3, false)) })
    .then("weights", { it.slide(1, optimizePortfolio()) }) // TODO we could do a weekly rebalancing instead of daily
    .join(["returns", "objective", "weights"], "portfolio", { it[0].slide(1, join(it[1], it[2])).sortColumns() }) // TODO we need a nice join operator here ....
    .pass(new Gnuplot(new File(PLOTPATH, "hrp-backtest.gnuplot"), new File("/tmp/hrp-backtest.jpg"), { [weights: it.columns() + 1] }))
    .then("current-weights", { weights -> weights.select({ it.startsWith("weight.") }).withRows([weights.lastRowKey()]).transpose() })
    .pass(new Gnuplot(new File(PLOTPATH, "hrp-current.gnuplot"), new File("/tmp/hrp-current.jpg")))
    .getPipelineResults()


println(experiment["portfolio"])

// if needed output some json files ...
//new File("rows.json").text = JsonOutput.toJson(joined.reshape.toMapOfRowMaps())
//new File(columns.json).text = JsonOutput.toJson(joined.reshape.toMapOfColumMaps())
println("done")



// Functions
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


def join(DataFrame objective, DataFrame targetWeights) {    // FIXME this join thingy needs to be refactored as well ... we need a proper join
    def portfolioPerformance = 0d;
    def oneOverNPerformance = 0d;

    return { DataFrame window, DataFrame result ->
        def lastRowKey = window.lastRowKey()
        DataFrame covariance = objective.getElement(lastRowKey, COVARIANCE)
        if (covariance?.columns() > 1) {
            // weigths
            LabeledMatrix targetWeightsVector = targetWeights.select(covariance.getColumnOrder())
                                                             .withRows([lastRowKey])
                                                             .toMatrix()

            LabeledMatrix returnsVector = window.select(covariance.getColumnOrder())
                                                .withRows([lastRowKey])
                                                .toMatrix()
                                                .transpose()

            LabeledMatrix oneOverNWeightsVector = returnsVector.elementWise {d -> 1d / returnsVector.rows()}
                                                               .transpose()

            // returns
            def portfolioReturn = targetWeightsVector.multiply(returnsVector).sum()
            def oneOverNReturn = oneOverNWeightsVector.multiply(returnsVector).sum()

            // risk
            def covarianceMatrix = covariance.toMatrix()
            def portfolioRisk = sqrt(targetWeightsVector.multiply(covarianceMatrix).multiply(targetWeightsVector.transpose()).sum())
            def oneOverNRisk = sqrt(oneOverNWeightsVector.multiply(covarianceMatrix).multiply(oneOverNWeightsVector.transpose()).sum())

            // performance
            portfolioPerformance = (1d + portfolioPerformance) * (1d + portfolioReturn) - 1d
            oneOverNPerformance = (1d + oneOverNPerformance) * (1d + oneOverNReturn) - 1d

            // fill result
            result.upsert(lastRowKey, "portfolio.risk", portfolioRisk * VaR95_Z_SCORE)
            result.upsert(lastRowKey, "portfolio.return", portfolioReturn)
            result.upsert(lastRowKey, "portfolio.performance", portfolioPerformance)
            result.upsert(lastRowKey, "one-over-n.risk", oneOverNRisk * VaR95_Z_SCORE)
            result.upsert(lastRowKey, "one-over-n.rerturn", oneOverNReturn)
            result.upsert(lastRowKey, "one-over-n.performance", oneOverNPerformance)
            targetWeightsVector.toDataframe().visit.walkRowWiseWithRowKey(
                    result,
                    {rk, df -> null},
                    {rk, ck, val, df -> df.upsert(rk, "weight." + ck, val)})

        }
    }
}

def covarianceCorrelationEstimator(int minSize, boolean demean = false) {
    return { DataFrame window, DataFrame result ->
        // check if we had enough price changes per asset to actually build a valid covariance matrix
        def validColumns = window.countStar().findAll {col, cnt -> cnt >= window.rows() - (window.rows() / covariance_max_0_percent)}

        // immediate return if we do not have enough data, i.e. ther is no point in optimizing a 1x1 Matrix
        if (validColumns.size() < minSize) return

        // estimate covarianec and correlation matrices and a hierachical cluster
        def returns = window.select(validColumns.keySet())
        def covarianceMatrix = returns.toMatrix().covariance(demean).toDataframe()
        def correlationMatrix = returns.toMatrix().correlation(demean).toDataframe()
        def clustered = correlationMatrix.clustering({ d -> sqrt(1d - d / 2d) }).cluster()
        result.upsert(window.lastRowKey(), COVARIANCE, covarianceMatrix)
        result.upsert(window.lastRowKey(), CORRELATION, correlationMatrix)
        result.upsert(window.lastRowKey(), CLUSTERED, clustered)
    }
}


def optimizePortfolio() {
    return { DataFrame window, DataFrame result ->
        DataFrame covariance = window.getElement(window.lastRowKey(), COVARIANCE)
        DataFrame clusteredCorrelation = window.getElement(window.lastRowKey(), CLUSTERED)
        def cItems = [clusteredCorrelation.getColumnOrder()]
        def weights = clusteredCorrelation.getColumnOrder().collectEntries { [(it): 1d] }

        while (cItems.size() > 0) {
            cItems = cItems.findAll { it.size() > 1 }
                           .collectMany { [it.subList(0, it.size().intdiv(2)), it.subList(it.size().intdiv(2), it.size())] }

            for (int i = 0; i < cItems.size(); i += 2) {
                def cItems0 = cItems[i]
                def cItems1 = cItems[i + 1]
                def cVar0 = inverseVariancePortfolio(covariance.select(cItems0).withRows(cItems0))
                def cVar1 = inverseVariancePortfolio(covariance.select(cItems1).withRows(cItems1))
                def alpha = 1 - cVar0 / (cVar0 + cVar1)
                cItems0.each { weights[it] *= alpha }
                cItems1.each { weights[it] *= 1 - alpha }
                // println("$cItems0 $cVar0 / $cItems1 $cVar1 / $alpha")
            }
        }

        weights.each { result.upsert(window.lastRowKey(), it.key, it.value) }
    }
}

def inverseVariancePortfolio(DataFrame cov) {
    def covMx = cov.toMatrix()
    def inverseDiag = covMx.getDiagonal(MatrixShape.ROW_VECTOR).elementWise({1d / it})
    def trace = inverseDiag.sum()
    def weights = inverseDiag.elementWise({ it / trace })
    return inverseVariance = weights.multiply(covMx).multiply(weights.transpose()).sum()
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
