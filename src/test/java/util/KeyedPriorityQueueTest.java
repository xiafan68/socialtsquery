package util;

import java.util.Random;
import java.util.TreeSet;

import org.junit.Assert;
import org.junit.Test;

public class KeyedPriorityQueueTest {
	@Test
	public void testSeqInsert() {
		KeyedPriorityQueue<Integer, Integer> queue = new KeyedPriorityQueue<Integer, Integer>();
		int count = 100;
		for (int i = 0; i < count; i++) {
			queue.offer(i, i);
		}

		for (int i = 0; i < count; i++) {
			Assert.assertTrue(queue.contains(99 - i));
			Assert.assertEquals(new Integer(99 - i), queue.poll());
		}
	}

	@Test
	public void testRandInsert() {
		KeyedPriorityQueue<Integer, Integer> queue = new KeyedPriorityQueue<Integer, Integer>();
		int count = 100;
		TreeSet<Integer> seg = new TreeSet<Integer>();
		Random rand = new Random();
		for (int i = 0; i < count; i++) {
			int next = rand.nextInt();
			if (!seg.contains(next)) {
				seg.add(next);
				queue.offer(next, next);
			}
		}

		while (!seg.isEmpty()) {
			Assert.assertTrue(queue.contains(seg.last()));
			Assert.assertEquals(seg.last(), queue.poll());
			seg.remove(seg.last());
		}
	}
}
