import org.junit.jupiter.api.Test;
import utils.NumberUtil;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class TestNumberUtil {

    @Test
    void testComb() {
        assertEquals(1, NumberUtil.comb(4, 0));
        assertEquals(4, NumberUtil.comb(4, 1));
        assertEquals(6, NumberUtil.comb(4, 2));
        assertEquals(4, NumberUtil.comb(4, 3));
        assertEquals(1, NumberUtil.comb(4, 4));
        assertEquals(1471429260, NumberUtil.comb(435, 4));
        assertEquals(0, NumberUtil.comb(4, 5));
    }

    @Test
    void testVarianceAndStdDev() {
        List<Double> data = new ArrayList<>();
        {
            System.out.println(data + " " + NumberUtil.mean(data) + " " + NumberUtil.variance(data) + " " + NumberUtil.stdDev(data));
            data.add(1.0);
            System.out.println(data + " " + NumberUtil.mean(data) + " " + NumberUtil.variance(data) + " " + NumberUtil.stdDev(data));
            data.add(2.0);
            System.out.println(data + " " + NumberUtil.mean(data) + " " + NumberUtil.variance(data) + " " + NumberUtil.stdDev(data));
            data.add(3.0);
            System.out.println(data + " " + NumberUtil.mean(data) + " " + NumberUtil.variance(data) + " " + NumberUtil.stdDev(data));
            data.add(4.0);
            System.out.println(data + " " + NumberUtil.mean(data) + " " + NumberUtil.variance(data) + " " + NumberUtil.stdDev(data));
            data.add(1.0);
            System.out.println(data + " " + NumberUtil.mean(data) + " " + NumberUtil.variance(data) + " " + NumberUtil.stdDev(data));
        }
    }

}
