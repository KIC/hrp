import kic.dataframe.DataFrame
import kic.dataframe.join.*
import kic.lppl.Sornette;

PATH = new File(getClass().protectionDomain.codeSource.location.path).getParentFile()
PLOTPATH = new File(PATH, "../gnuplot/")

def symbol = "^spx" // "^spx"
def from = "20150101"
def prices = getPriceDataFrame("BTC", "USD", from)
println(prices)
println("---")

def start = System.currentTimeMillis()
def columns = ["CLOSE"] //["OPEN", "HIGH", "LOW", "CLOSE"]
def s = prices.slide.slide(510, sornette(columns, 26, 10))
s = s.sortRows()
def stop = System.currentTimeMillis()
println(s)

def result = new Join(s, "heat")
        .left(prices, "price", new OuterJoinFillLastRow({l, r -> r}))
        .getJoinedDataFrame()
        .slide.rows({ df ->
            [price: df.getElement(df.lastRowKey(), "price").getElement(df.lastRowKey(),"CLOSE"),
             heat: df.getElement(df.lastRowKey(), "heat").getElement(df.lastRowKey(), "TC")
            ]
        })
        .select(["price", "heat"])

println(result)
println("convert keys using milliseconds: new Date(17516 * 24 * 3600 * 1000)")
println("fitting took ${(stop - start) / 1000 / 60}min.")
new File("/tmp/bitcoin-heat.csv").text = result.toString()
new Gnuplot(new File(PLOTPATH, "sornette-heatmap.gnuplot"), new File("/tmp/bitcoin-heat.jpg")).accept(result)

// Functions
DataFrame getPriceDataFrame(currency, referenceCurrency, from) {
    CryptoCompare cc = new CryptoCompare()
    DataFrame prices = new DataFrame()

    cc.getHistData(currency, referenceCurrency, "day", 0, "CCCAGG").forEach { bar ->
//        prices.upsert(bar.time, currency, bar.close)
        prices.upsert(bar.time as Long, "TIME", (bar.time as double) / 60 / 60 / 24)
        prices.upsert(bar.time as Long, "CLOSE", bar.close as double)

    }

    return prices
}

def sornette(columns, int nrOfSubwindows, int step = 1) {
    def solver = Sornette.newDefaultSolver([] as double[], [] as double[])

    return { DataFrame prices, DataFrame result ->
        def rowKeys = prices.rowOrder
        if (result.withRows([prices.lastRowKey()]).rows() <= 0) result.upsert(prices.lastRowKey(), "TC", 0)

        for (int i = 0; i < nrOfSubwindows; i++) {
            def rows = rowKeys.subList(step * i, rowKeys.size())

            for (String col : columns) {
                def timePriceMatrix = prices.select(["TIME", col]).withRows(rows).toMatrix().transpose().to2DArray();
                def solution = solver.withNewTarget(timePriceMatrix[0], timePriceMatrix[1], i > 0).solve()
                def tc = (solution[2]) as long
                def counter = result.withDefault(0).getElement(tc, "TC") + ((tc - prices.lastRowKey() > 1) ? 1 : 0)

                println("${prices.lastRowKey()}: $solution")
                result.upsert(tc, "TC", counter)
            }
        }

    }
}
