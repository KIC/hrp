package scratch;

import org.junit.Test;
import org.math.plot.Plot2DPanel;

import javax.swing.*;
import java.io.IOException;
import java.util.Arrays;

/**
 * Created by kindler on 15/09/2017.
 */
public class PlotTest {

    @Test
    public void fooTest() {
        double[] arr = new double[2];
        int j = 1;
        arr[0] = 1.22;
        arr[j++] = arr[j - 2];
        System.out.println(Arrays.toString(arr));

        int[] iarr = new int[1];
        int i=0;
        iarr[i++] = i;
        System.out.println(Arrays.toString(iarr));
    }

    @Test
    public void linePlot() throws IOException {
        // create your PlotPanel (you can use it as a JPanel)
        Plot2DPanel plot = new Plot2DPanel();

        plot.addLinePlot("lala", new double[][]{new double[]{1,0.2}, new double[]{2,0.3}});

        // put the PlotPanel in a JFrame like a JPanel
        new Thread(){
            @Override
            public void run() {
                JFrame frame = new JFrame("a plot panel");
                frame.setSize(600, 600);
                frame.setContentPane(plot);
                frame.setVisible(true);
            }
        }.start();

        System.in.read();
    }
}
