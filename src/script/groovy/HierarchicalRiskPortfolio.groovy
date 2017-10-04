#!/usr/bin/env groovy
import groovy.json.JsonOutput
import static groovy.lang.Closure.IDENTITY
import org.apache.commons.math3.linear.MatrixUtils

import java.time.*
import static java.lang.Math.*

//@Grab(group='net.sourceforge.parallelcolt', module='parallelcolt', version='0.10.0')  //needed by joptimizer
import cern.colt.matrix.tdouble.*
import cern.jet.math.tdouble.*
//@Grab(group='com.joptimizer', module='joptimizer', version='4.0.0')
import com.joptimizer.functions.*
import com.joptimizer.optimizers.*

//@Grab(group='com.apporiented' , module='hierarchical-clustering', version='1.1.1-SNAPSHOT')
//@Grab(group='kic', module='dataframe', version='1.0-SNAPSHOT', changing=true)
import kic.dataframe.*
import kic.dataframe.join.*
import kic.dataframe.linalg.*
import kic.pipeline.PipelineExecutor
import static kic.dataframe.Operations.*

// TODO Next
// TODO make rebalancing happening on sundays
// TODO add trading costs
// TODO also plot a video of the clustered correlation matrix like so:
// http://www.gnuplotting.org/animation-video/ || http://www.gnuplotting.org/tag/animation/ && http://gnuplot.sourceforge.net/demo_svg/heatmaps.html
// TODO add a last step in how many coins we need to buy with respect to the most recent realtime price
// TODO add sonrette indicator

// runtime parameters - later parse this from commandline: http://mrhaki.blogspot.ch/2009/09/groovy-goodness-parsing-commandline.html
currencies = ["BTC", "LTC", "ETH", "XRP", "XMR", "DASH", "REP", "ZEC", "ETC"]
referenceCurrency = "EUR"
exchange="CCCAGG"
frequency = "day"
covariance_window = 60
covariance_correct_bias = true
covariance_max_0_percent = 0.15
min_assets = 3
trading_costs = 0.0016 // 0.16% on kraken

// constants
PATH = new File(getClass().protectionDomain.codeSource.location.path).getParentFile()
PLOTPATH = new File(PATH, "../gnuplot/")
COVARIANCE = "COVARIANCE"
CORRELATION = "CORRELATION"
CLUSTERED = "CLUSTERED"
MEANRETURNS = "MEANRETURNS"
VaR95_Z_SCORE = 1.645 // stddev to VaR 95 factor

println("running at: $PATH\nusing temp: ${System.getProperty('java.io.tmpdir')}")

// NOTE we have weired huge jumps i.e. dash jumps 1400% in one day, so we ignore 3 fold and bigger jumps in prices
def experiment = PipelineExecutor
    .startFrom("prices", getPriceDataFrame(currencies))
    .thenCalculate("returns", { prices -> prices.slide.slide(2, deltaOperation({ sell, buy -> sell != buy && sell < 4 * buy ? sell / buy - 1d : null }))})
    .thenCalculate("objective", { returns -> returns.slide.window(covariance_window, covarianceCorrelationReturnsEstimator(min_assets, covariance_correct_bias)) })
    .thenCalculate("hrpweights", { objective -> objective.slide.rows(optimizeHierarchicalRiskPortfolio()) })
        .and("meanvarainceweights", { objective -> objective.slide.rows(optimizeMeanVariancePortfolio())})
    .join("returnsandweights", "objective", { objective -> objective["-1:$COVARIANCE"] }, [
        returns: new OuterJoinFillLastRow({ l, r -> r.select(l.getColumnOrder()) }),
        hrpweights: new OuterJoinFillLastRow({ l, r -> r.select(l.getColumnOrder()) }),
        meanvarainceweights: new OuterJoinFillLastRow({ l, r -> r.select(l.getColumnOrder()) })
    ])
    .thenCalculate("backtest", { rw -> rw.slide.rows(backtest()).sortColumns() })
    .passOverTo(new Gnuplot(new File(PLOTPATH, "hrp-backtest.gnuplot"), new File("/tmp/hrp-backtest-cov-$covariance_window-${covariance_correct_bias ? "corrected" : "biased"}.jpg"), { [weights: currencies.size() ] }))
    .thenCalculate("currentweights", { weights -> weights.select({ it.startsWith("weight in HRP") }).withRows([weights.lastRowKey()]).transpose() })
    .passOverTo(new Gnuplot(new File(PLOTPATH, "hrp-current.gnuplot"), new File("/tmp/hrp-current-$covariance_window-${covariance_correct_bias ? "corrected" : "biased"}.jpg")))
    //.thenCalculate("coins", {claculate needed coins}
    .getPipelineResults()


println(experiment["backtest"].withDefault(0d))

// if needed output some json files ...
//new File("rows.json").text = JsonOutput.toJson(joined.reshape.toMapOfRowMaps())
//new File(columns.json).text = JsonOutput.toJson(joined.reshape.toMapOfColumMaps())
println(experiment["prices"])
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

def backtest() {
    def hrpPerformance = 0d;
    def meanVariancePerformance = 0d;
    def oneOverNPerformance = 0d;

    return { window ->
        LabeledMatrix returns = window["-1:returns"].toMatrix()
        LabeledMatrix covariance = window["-1:objective"].toMatrix()
        LabeledMatrix hrpWeights = window["-1:hrpweights"].toMatrix()
        LabeledMatrix meanVarainceWeights = window["-1:meanvarainceweights"].toMatrix()
        LabeledMatrix oneOverNWeights = returns.elementWise({ d -> 1d / returns.columns() })

        // returns
        returns = returns.transpose()
        def hrpReturn = hrpWeights.multiply(returns).sum()
        def meanVarianceReturn = meanVarainceWeights.multiply(returns).sum()
        def oneOverNReturn = oneOverNWeights.multiply(returns).sum()

        // risk
        def hrpRisk = sqrt(hrpWeights.multiply(covariance).multiply(hrpWeights.transpose()).sum()) * VaR95_Z_SCORE
        def meanVarianceRisk = sqrt(meanVarainceWeights.multiply(covariance).multiply(meanVarainceWeights.transpose()).sum()) * VaR95_Z_SCORE
        def oneOverNRisk = sqrt(oneOverNWeights.multiply(covariance).multiply(oneOverNWeights.transpose()).sum()) * VaR95_Z_SCORE

        // performance
        hrpPerformance = (1d + hrpPerformance) * (1d + hrpReturn) - 1d
        meanVariancePerformance = (1d + meanVariancePerformance) * (1d + meanVarianceReturn) - 1d
        oneOverNPerformance = (1d + oneOverNPerformance) * (1d + oneOverNReturn) - 1d

        return ["HRP Performance": hrpPerformance, "HRP VaR": hrpRisk,
                "MinVar Performance": meanVariancePerformance, "Min Variance VaR": meanVarianceRisk,
                "1/N Performance": oneOverNPerformance, "1/N VaR": oneOverNRisk
               ] << (hrpWeights.getRowAsMap(0, {"weight in HRP $it"})
                 <<  meanVarainceWeights.getRowAsMap(0, {"weight in MinVar $it"})
                 <<  oneOverNWeights.getRowAsMap(0, {"weight in 1/N $it"}))
    }
}

def covarianceCorrelationReturnsEstimator(int minSize, boolean demean = false) {
    return { DataFrame window ->
        // check if we had enough price changes per asset to actually build a valid covariance matrix
        def validColumns = window.countStar().findAll {col, cnt -> cnt >= window.rows() - (window.rows() * covariance_max_0_percent)}

        // immediate return if we do not have enough data, i.e. there is no point in optimizing a 1x1 Matrix
        if (validColumns.size() < max(2, minSize)) return [:]

        // estimate covarianec and correlation matrices and a hierachical cluster
        def returns = window.select(validColumns.keySet()).toMatrix()  // mean returns would be 1/n * returns, where n = returns_m
        def meanReturns = returns.getColumnAsRowVector(0).elementWise({ d  -> 1d / returns.rows() }).multiply(returns).toDataframe()
        def covarianceMatrix = returns.covariance(demean).toDataframe()
        def correlationMatrix = returns.correlation(demean).toDataframe()
        def clustered = correlationMatrix.clustering({ d -> sqrt(1d - d / 2d) }).cluster()
        return [(COVARIANCE): covarianceMatrix, (CORRELATION): correlationMatrix, (CLUSTERED): clustered, (MEANRETURNS): meanReturns]
    }
}

def isSunday(int unixTimeStamp) {
    def dt = LocalDateTime.ofInstant(Instant.ofEpochSecond(unixTimeStamp), ZoneOffset.UTC)
    return dt.dayOfWeek.value == 7
}

def optimizeHierarchicalRiskPortfolio() {
    return { DataFrame window ->
        DataFrame covariance = window["-1:$COVARIANCE"] //window.getElement(window.lastRowKey(), COVARIANCE)
        DataFrame clusteredCorrelation = window["-1:$CLUSTERED"] // window.getElement(window.lastRowKey(), CLUSTERED)
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
            }
        }

        return weights
    }
}

def inverseVariancePortfolio(DataFrame cov) {
    def covMx = cov.toMatrix()
    def inverseDiag = covMx.getDiagonal(MatrixShape.ROW_VECTOR).elementWise({1d / it})
    def trace = inverseDiag.sum()
    def weights = inverseDiag.elementWise({ it / trace })
    return inverseVariance = weights.multiply(covMx).multiply(weights.transpose()).sum()
}


def optimizeMeanVariancePortfolio(double lamda = 0d) {
    return { DataFrame window ->
        LabeledMatrix covariance = window["-1:$COVARIANCE"].toMatrix()
        LabeledMatrix meanReturns = window["-1:$MEANRETURNS"].select(covariance.columnLabels).toMatrix()

        // minimize: w'Ʃx + λw'r
        // s.t.: w >= 0
        def sigma = DoubleFactory2D.dense.make(covariance.to2DArray())
        def expectedReturns = DoubleFactory1D.dense.make(meanReturns.to2DArray()[0]).assign(DoubleFunctions.mult(-lamda))
        def objective = new PDQuadraticMultivariateRealFunction(sigma, expectedReturns, 0)
        def inequalities = MatrixUtils.createRealIdentityMatrix(expectedReturns.size() as int)
                                      .scalarMultiply(-1d)
                                      .getData()
                                      .collect { new LinearMultivariateRealFunction(it, 0) } as ConvexMultivariateRealFunction[]

        def or = new OptimizationRequest(f0: objective, fi: inequalities)
        def optimizer = new JOptimizer(request: or)
        optimizer.optimize()

        // NOTE instead of constraining the weights to be equal to 1 it is more efficiton to just let them go and rescale them afterwards
        def unscaledWeights = optimizer.getOptimizationResponse().getSolution()
        def norm = unscaledWeights.sum()

        return [meanReturns.columnLabels, unscaledWeights].transpose().collectEntries { [(it[0]): it[1] / norm]}
    }
}