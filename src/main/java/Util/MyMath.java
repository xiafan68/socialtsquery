package Util;

/**
 * Created by teisei on 15-3-25.
 */
public class MyMath {
    private static int pow2[] = new int[32];
    static {
        for (int i = 0; i < 32; i++)
            pow2[i] = (int) Math.pow(2, i);
    }
    /**
     * equals:(int) Math.ceil(Math.log(maxLifeTime) / Math.log(2));
     */
    public static int getCeil(double maxLifeTime) {
        for (int i = 1; i < 32; i++) {
            if (pow2[i] > maxLifeTime || pow2[i] == maxLifeTime) {
                return i;
            }
        }
        return -1;
    }
}
