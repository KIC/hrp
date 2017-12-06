import kic.dataframe.DataFrame
import kic.dataframe.join.*
import kic.dataframe.linalg.LabeledMatrix
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.DecompositionSolver;
import org.apache.commons.math3.linear.LUDecomposition;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.apache.commons.math3.linear.SingularMatrixException;
import org.apache.commons.math3.linear.SingularValueDecomposition;
import net.finmath.optimizer.LevenbergMarquardt;
import net.finmath.optimizer.SolverException;
import static org.apache.commons.math3.util.FastMath.cos;
import static org.apache.commons.math3.util.FastMath.log;
import static org.apache.commons.math3.util.FastMath.pow
import static org.apache.commons.math3.util.FastMath.round;
import static org.apache.commons.math3.util.FastMath.sin;


referenceCurrency = "EUR"
exchange="CCCAGG"

def priceBTC = getPriceDataFrame("BTC")
println(priceBTC)
println("---")

def columns = ["CLOSE", "HIGH", "LOW"]
def s = priceBTC.slide.slide(360, sornette(columns, 360, 10, 12))
s = s.sortRows()
println(s)

def result = new Join(s, "heat")
        .left(priceBTC, "price", new OuterJoinFillLastRow({l, r -> r}))
        .getJoinedDataFrame()
        .slide.rows({ df ->
            [price: df.getElement(df.lastRowKey(), "price").getElement(df.lastRowKey(),"CLOSE"),
             heat: df.getElement(df.lastRowKey(), "heat").getElement(df.lastRowKey(), "TC")
            ]
        })

println(result)
new File("/tmp/price-heat.csv").text = result.toString()

DataFrame getPriceDataFrame(currency) {
    CryptoCompare cc = new CryptoCompare()
    DataFrame prices = new DataFrame()
    def since = 1500000000
    //def since = 1506214800

    cc.getHistData(currency, referenceCurrency, "hour", since, exchange).forEach { bar ->
        prices.upsert(bar.time as Long, "TIME", bar.time / 60d / 60d as double)
        prices.upsert(bar.time as Long, "CLOSE", bar.close as double)
        prices.upsert(bar.time as Long, "HIGH", bar.high as double)
        prices.upsert(bar.time as Long, "LOW", bar.low as double)
        prices.upsert(bar.time as Long, "LOG_CLOSE", log(bar.close as double))
        prices.upsert(bar.time as Long, "LOG_HIGH", log(bar.high as double))
        prices.upsert(bar.time as Long, "LOG_LOW", log(bar.low as double))
    }

    return prices
}

// implementaion of https://www.sg.ethz.ch/ethz_risk_center_wps/pdf/ETH-RC-11-002.pdf
def sornette(columns, int window, int nrOfSubwindows, int step = 1) {
    if (nrOfSubwindows * step >= window) throw new IllegalArgumentException("subwindows * step needs to be smaller as window!")
    double lastM = 0.5d
    double lastW = 9d

    return { DataFrame prices, DataFrame result ->
        def rowKeys = prices.rowOrder
        for (int i = 0; i < nrOfSubwindows; i++) {
            def rows = rowKeys.subList(step * i, rowKeys.size())
            def res = columns.collect { new LogPeriodic(prices.select(["TIME", it]).withRows(rows).toMatrix(), lastM, lastW, 4).solve() }

            res.tc.each {
                result.upsert(it, "TC", result.withDefault(0).getElement(it, "TC") + ((it - prices.lastRowKey() > 0) ? 1 : 0))
                println(result.withRows([(it)]))
            }

            lastM = res.m.sum() / columns.size()
            lastW = res.w.sum() / columns.size()
        }

    }
}

class LogPeriodic extends LevenbergMarquardt {
    final LabeledMatrix xyDataPoints
    final ABCC abcc

    // initial guess if tc provided then:
    //   new double[]{0.5,9d};
    // else
    //   new double[]{0.5,9d,2014.5};
    //LogPeriodic(int threads, Double tc, double[] initialguess, double[] decTime, double[] price, double[] weights, boolean lnPrice) {
    LogPeriodic(LabeledMatrix xyDataPoints, double initGuessedM = 0.5, double initGuessedW = 9, int threads = 1) {
        super(threads)
        this.xyDataPoints = xyDataPoints
        this.abcc = new ABCC(xyDataPoints)
        this.setMaxIteration(2000)
        this.setErrorTolerance(0.01d)
        this.setWeights((0..xyDataPoints.rows()).collect {1d} as double[])
        this.setInitialParameters([initGuessedM, initGuessedW, xyDataPoints.getElement(xyDataPoints.rows() - 1, 0) + 1d] as double[])
        this.setTargetValues(xyDataPoints.getColumnAsRowVector(1).to2DArray()[0])
    }

    double evaluate(double t, double... parameters) {
        def m = parameters[0]
        def w = parameters[1]
        def tc = parameters[2]

        // solve the linear parameters
        abcc.calcABCC(tc, m, w)
        def a = pow((tc - t), m)

        // return A + B * a + C1 * a * cos(w * log(tc - t)) + C2 * a * sin(w * log(tc - t))
        return abcc.a + abcc.b * a + abcc.c1 * a * cos(w * log(tc - t)) + abcc.c2 * a * sin(w * log(tc - t))
    }

    @Override
    void setValues(double[] parameters, double[] values) {
        for (int i = 0; i < values.length; i++) {
            values[i] = evaluate(xyDataPoints.getElement(i, 0), parameters);
        }
    }

    Map solve() throws SolverException {
        run();
        double[] solution = getBestFitParameters();
        return [m: solution[0], w: solution[1], tc: round(solution[2]) * 60 * 60];
    }

}

public class ABCC {
    private final double[] T,p;
    private final int N;
    public double a,b,c1,c2;

    public ABCC(LabeledMatrix xyDataPoints) {
        this.T = xyDataPoints.getColumnAsRowVector(0).to2DArray()[0]
        this.p = xyDataPoints.getColumnAsRowVector(1).to2DArray()[0]
        this.N = p.length;
    }

    public void calcABCC(double tc, double m, double w) {
        double sum_fi=0,sum_gi=0,sum_fi2=0,sum_fi_gi=0,sum_gi2=0,sum_yi=0,sum_yi_fi=0,sum_yi_gi=0,sum_hi=0,sum_fi_hi=0,sum_gi_hi=0,sum_hi2=0,sum_yi_hi=0;

        for (int i=0;i<N;i++) {
            double t = T[i];
            double fi = pow((tc - t),m);
            double gi = fi * cos(w * log(tc - t));
            double hi = fi * sin(w * log(tc - t));
            double yi = p[i];

            sum_fi += fi;
            sum_gi += gi;
            sum_fi2 += fi * fi;
            sum_fi_gi += fi * gi;
            sum_gi2 += gi * gi;
            sum_yi += yi;
            sum_yi_fi += yi * fi;
            sum_yi_gi += yi * gi;
            sum_hi += hi;
            sum_fi_hi += fi * hi;
            sum_gi_hi += gi * hi;
            sum_hi2 += hi * hi;
            sum_yi_hi += yi * hi;
        }

        RealMatrix coefficients = new Array2DRowRealMatrix([
            [ N,         sum_fi,     sum_gi,        sum_hi ],
            [ sum_fi,    sum_fi2,    sum_fi_gi,     sum_fi_hi ],
            [ sum_gi,    sum_fi_gi,  sum_gi2,       sum_gi_hi ],
            [ sum_hi,    sum_fi_hi,  sum_gi_hi,     sum_hi2 ]] as double[][],
            false
        );

        DecompositionSolver solver = new LUDecomposition(coefficients).getSolver();

        //Next create a RealVector array to represent the constant vector B and use solve(RealVector) to solve the system
        RealVector constants = new ArrayRealVector([ sum_yi, sum_yi_fi, sum_yi_gi, sum_yi_hi ] as double[], false);
        RealVector solution;

        try {
            solution = solver.solve(constants);
        } catch (SingularMatrixException sme) {
            // on singular matrix try a different solver!
            DecompositionSolver solver2 = new SingularValueDecomposition(coefficients).getSolver();
            solution = solver2.solve(constants);
        }

        this.a = solution.getEntry(0);
        this.b = solution.getEntry(1);
        this.c1 = solution.getEntry(2);
        this.c2 = solution.getEntry(3);
    }
}
