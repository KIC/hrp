import kic.dataframe.DataFrame
import kic.dataframe.linalg.Rescale
import org.apache.commons.math3.ml.clustering.*
import org.apache.commons.math3.ml.distance.EarthMoversDistance
import org.apache.commons.math3.ml.distance.ManhattanDistance

import static java.lang.Math.*
import static kic.dataframe.Operations.deltaOperation
import org.apache.commons.math3.ml.clustering.DoublePoint

PATH = new File(getClass().protectionDomain.codeSource.location.path).getParentFile()

def symbol = "^spx" // "^dji"
DataFrame prices = getPriceDataFrame(symbol)
println(prices)

// calculate delta bars relative to last close and thenCalculate
// convert these delta bars to org.apache.commons.math4.ml.clustering.DoublePoint(double[])
DataFrame deltaBars = prices.slide.slide(2, deltaOperation({ sell, buy ->
    new DeltaBar(sell.symbol.toString(),
                 sell.open   / buy.close  - 1d as double,
                 sell.high   / buy.close  - 1d as double,
                 sell.low    / buy.close  - 1d as double,
                 sell.close  / buy.close  - 1d as double,
                 sell.volume / buy.volume - 1d as double)}))

println(deltaBars)

def classed = [:]
deltaBars.getData().each {
    def list = classed[it.classifier]
    classed[it.classifier] = list == null ? [it] : list << it
}

classed.each { classifier, List<DeltaBar> bars ->
    println("rule sample: ${bars.size()}")
    def filename = "/tmp/bars/r.${classifier}.csv"
    new File(filename).withWriter { out ->
        bars.eachWithIndex { it, idx ->
            out.println("$idx,${it.open},${it.high},${it.low},${it.close},${it.volume}")
        }
    }

    def cmd = "cmd /C gnuplot -e \"filename='$filename'\" \"${new File(PATH, "../gnuplot/candlesticks.gnuplot").toString()}\" > ${filename}.jpg"
    println(cmd)
    cmd.execute()
}

/*


// classify all delta bars
// run a k-means // http://commons.apache.org/proper/commons-math/userguide/ml.html
// also try DBSCANClusterer
// we could add a weighted volume like volume * 0.001
// we could try to use the manhatten distance to keep the negatives (instead of squaring them away)
KMeansPlusPlusClusterer kClusterer = new KMeansPlusPlusClusterer(8, -1, new ManhattanDistance()) // use a fibonacci number :-)
def kClusters = kClusterer.cluster(deltaBars.getData())
println(kClusters)

DBSCANClusterer dbClusterer = new DBSCANClusterer(2.0, 10, new EarthMoversDistance()) // this setting somehow works besst
def dbClusters = dbClusterer.cluster(deltaBars.getData())
println(dbClusters)

println("sample")
println("Candles,Open,High,Low,Close,Volume")

// plot delta bars per classifier
kClusters.eachWithIndex { c, index ->
    println("k-mean sample: ${c.points.size()}")
    def filename = "/tmp/k.${index}.csv"
    new File(filename).withWriter { out ->
        c.points.eachWithIndex { it, idx ->
            //println("0,${it.open},${it.high},${it.low},${it.close},${it.volume}")
            out.println("$idx,${it.open},${it.high},${it.low},${it.close},${it.volume}")
        }
    }

    "cmd /C gnuplot -e \"filename='$filename'\" \"${new File(PATH, "../candlesticks.gnuplot").toString()}\" > ${filename}.jpg".execute()
}

dbClusters.eachWithIndex { c, index ->
    println("db sample: ${c.points.size()}")
    def filename = "/tmp/db.${index}.csv"
    new File(filename).withWriter { out ->
        c.points.eachWithIndex { it, idx ->
            //println("0,${it.open},${it.high},${it.low},${it.close},${it.volume}")
            out.println("$idx,${it.open},${it.high},${it.low},${it.close},${it.volume}")
        }
    }

    "cmd /C gnuplot -e \"filename='$filename'\"  \"${new File(PATH, "../candlesticks.gnuplot").toString()}\" > ${filename}.jpg".execute()
}

println("done")
*/


// Functions
DataFrame getPriceDataFrame(symbol) {
    def yf = new Stooq()
    DataFrame prices = new DataFrame()

    yf.getHistData(symbol,"", true, true).forEach { bar ->
        prices.upsert(bar.date, symbol, bar)
    }

    return prices
}

class DeltaBar extends DoublePoint {
    final String symbol
    final double open, high, low, close, volume
    final int color, classifier

    //TODO try to rescale from low/high to -1d/1d
    DeltaBar(String symbol, double open, double high, double low, double close, double volume) {
        //super([sell.close > buy.close ? 1d : sell.close < buy.close ? -1d : 0d, open, high, low, close] as double[])
        //super(([close > open ? 1d : close < open ? -1d : 0d, open, open / high - 1d, pow(open / close, 2d), close / low] as double[]))
        super(new Rescale(low, high, -1d, 1d).rescale([close > open ? 0.5 * low : close < open ? 2 * high : 0d, open, open / high - 1d, pow(open / close, 2d), close / low] as double[]))
        //super(([close > open ? 1d : close < open ? -1d : 0d, open / close] as double[]))
        this.symbol = symbol
        this.open = open
        this.high = high
        this.low = low
        this.close = close
        this.volume = volume
        this.color = abs(open / close - 1d) < 0.001 ? 1 : close > open ? 2 : 3
        this.classifier = color * 10000 +
                          new Rescale(low, high, (100..1200).step(100) as int[]).discretize(open) +
                          new Rescale(low, high, (1..12) as int[]).discretize(close)
    }

    @Override
    String toString() {
        return "$symbol: $open, $high, $low, $close, $volume"
    }
}
