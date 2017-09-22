package kic.dataframe.linalg;

/**
 * Created by kindler on 20/09/2017.
 */
public class Rescale {
    private final double range0,range1,domain0,domain1;

    // input == domain, output == range
    public Rescale(double domain0, double domain1, double range0, double range1) {
        this.range0 = range0;
        this.range1 = range1;
        this.domain0 = domain0;
        this.domain1 = domain1;
    }

    private double interpolate(double x) {
        return range0 * (1 - x) + range1 * x;
    }

    private double uninterpolate(double x) {
        double b = (domain1 - domain0) != 0 ? domain1 - domain0 : 1 / domain1;
        return (x - domain0) / b;
    }

    public double rescale(double x) {
        return interpolate(uninterpolate(x));
    }

    public double[] rescale(double[] x) {
        double[] rescaled = new double[x.length];
        for (int i = 0; i < x.length; i++) rescaled[i] = rescale(x[i]);
        return rescaled;
    }

    public double[][] rescale(double[][] x) {
        double[][] rescaled = new double[x.length][];
        for (int i = 0; i < x.length; i++) rescaled[i] = rescale(x[i]);
        return rescaled;
    }
}
