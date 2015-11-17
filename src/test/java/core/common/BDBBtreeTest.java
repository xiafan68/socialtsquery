package core.common;

import core.commom.BDBBtree;
import core.io.Bucket;
import core.lsmt.WritableComparableKey;
import util.Configuration;
import util.Pair;

import org.junit.Assert;
import org.junit.Test;

import java.io.*;

/**
 * Created by teisei on 9/11/15.
 */
public class BDBBtreeTest {
    @Test
    public void testInsert() throws IOException {

//        System.setOut(new PrintStream(new FileOutputStream("~/tmp/7_0.txt")));
        Configuration conf = new Configuration();
        conf.load("conf/index.conf");


        BDBBtree bdbBtree = new BDBBtree(conf.getIndexDir(), conf);
        bdbBtree.open(false, true);

        String key[] = new String[]{"hello", "world", "come"};
        String value[] = new String[]{"12", "13", "14"};
        int buckid1[] = new int[]{1, 1, 1};
        int buckid2[] = new int[]{2, 2, 2};
        WritableComparableKey.StringKey curkeys[] = new WritableComparableKey.StringKey[3];
        WritableComparableKey.StringKey curvalues[] = new WritableComparableKey.StringKey[3];
        Bucket.BucketID bucketIDs[] = new Bucket.BucketID[3];
        for (int i = 0; i < key.length; i++) {
            curkeys[i] = new WritableComparableKey.StringKey(key[i]);
            curvalues[i] = new WritableComparableKey.StringKey(value[i]);
            bucketIDs[i] = new Bucket.BucketID(buckid1[i], (short) buckid2[i]);


            bdbBtree.insert(curkeys[i], curvalues[i], bucketIDs[i]);
        }


        System.out.println("test floor:");
        //Assert.assertTrue(getFloorPair(bdbBtree, "hello","111").getValue().compareTo());
//        WritableComparableKey.StringKey curvalue1 = new WritableComparableKey.StringKey("111");
//        Pair<WritableComparableKey, Bucket.BucketID> pair = bdbBtree.floorOffset(curkey, curvalue1);
//        System.out.println(pair.getValue());


//        System.out.println("test floor:");
//        WritableComparableKey.StringKey curvalue2 = new WritableComparableKey.StringKey("111");
//        Pair<WritableComparableKey, Bucket.BucketID> pair2 = bdbBtree.floorOffset(curkey, curvalue2);
//
//        Assert.assertTrue(pair.getValue().compareTo(bucketID) == 0);
    }


    public Pair<WritableComparableKey, Bucket.BucketID> getFloorPair(BDBBtree bdbBtree, String key, String value) throws IOException {
        WritableComparableKey.StringKey curkey = new WritableComparableKey.StringKey(key);
        WritableComparableKey.StringKey curvalue = new WritableComparableKey.StringKey(value);
        return bdbBtree.floorOffset(curkey, curvalue);
    }
    public Pair<WritableComparableKey, Bucket.BucketID> getCellPair(BDBBtree bdbBtree, String key, String value) throws IOException {
        WritableComparableKey.StringKey curkey = new WritableComparableKey.StringKey(key);
        WritableComparableKey.StringKey curvalue = new WritableComparableKey.StringKey(value);
        return bdbBtree.cellOffset(curkey, curvalue);
    }
}
