package common;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import segmentation.Segment;
import util.Pair;

public class TestDataGenerator {
	TestDataGeneratorBuilder builder;

	Set<Long> genedMids = new HashSet<Long>();
	long curMid = -1;
	int curMidSegNum = -1;
	int curMidGenSegNum = 0;
	Set<String> curTerms = new HashSet<String>();
	int lastStart = 0;
	int lastTGap = 0;
	int lastCount = 0;

	Random rand;

	public TestDataGenerator(TestDataGeneratorBuilder builder) {
		rand = new Random(builder.getSeed());
		this.builder = builder;
	}

	public boolean hasNext() {
		return genedMids.size() < builder.maxMidNum || curMidGenSegNum < curMidSegNum;
	}

	private void genNextMid() {
		do {
			curMid = Math.abs(rand.nextLong()) % builder.maxMidNum;
		} while (genedMids.contains(curMid));
		genedMids.add(curMid);
	}

	public Pair<Set<String>, MidSegment> nextData() {
		if (curMidGenSegNum >= curMidSegNum) {
			genNextMid();
			curMid = Math.abs(rand.nextLong()) % builder.getMaxMidNum();
			curMidGenSegNum = 0;
			curMidSegNum = Math.abs(rand.nextInt()) % builder.getMaxSegNum();
			int termNum = Math.abs(rand.nextInt()) % builder.getMaxTermNum();
			curTerms.clear();
			for (int i = 0; i < termNum; i++) {
				curTerms.add(Integer.toString(Math.abs(rand.nextInt()) % builder.getMaxTerm()));
			}
			lastStart = Math.abs(rand.nextInt()) % builder.getMaxTime();
			lastTGap = 0;
			lastCount = Math.abs(rand.nextInt()) % builder.getMaxCount();
		}

		curMidGenSegNum++;
		lastStart = lastStart + lastTGap;
		lastTGap = Math.abs(rand.nextInt()) % builder.getMaxIntervalLen();
		int cgap = rand.nextInt() % builder.getMaxCount();
		MidSegment seg = new MidSegment(rand.nextLong(),
				new Segment(lastStart, lastCount, lastStart + lastTGap, Math.max(0, lastCount + cgap)));
		lastCount = Math.max(0, lastCount + cgap);
		return new Pair<>(curTerms, seg);
	}
}
