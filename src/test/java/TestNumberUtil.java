import org.junit.jupiter.api.Test;
import utils.NumberUtil;

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

}
