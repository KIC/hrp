package jvm;

import org.junit.Assert;
import org.junit.Test;


/**
 * Created by kindler on 17/09/2017.
 */
public class IncrementerTest {
    @Test
    public void testIncrement() {
        int[] iarr = new int[1];
        int i=0;
        iarr[i++] = i;  // this is very nasty but i relay on this behavior so i hope this is true across all jvm implementations
        Assert.assertEquals(1, iarr[0]);
    }
}
