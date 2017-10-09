package common;

public class TestDataGeneratorBuilder {
	int seed;
	// the maximum number of terms that the content of a single social item
	// contains
	int maxTermNum = 10;
	// the maximum number of segments that a single social item has
	int maxSegNum = 100;
	// the maximum number of terms contained in the global vocabulary
	int maxTerm = 30;
	// the maximum number of unique social items
	int maxMidNum = 100000;
	// the maximum time window
	int maxTime = 10000;
	// the maximum interval length of segments
	int maxIntervalLen = 100;
	// the maximum count a segment can have at two end points
	int maxCount = 100;

	public static TestDataGeneratorBuilder create() {
		return new TestDataGeneratorBuilder();
	}

	public TestDataGenerator build() {
		return new TestDataGenerator(this);
	}

	public int getSeed() {
		return seed;
	}

	public TestDataGeneratorBuilder setSeed(int seed) {
		this.seed = seed;
		return this;
	}

	public int getMaxTermNum() {
		return maxTermNum;
	}

	public TestDataGeneratorBuilder setMaxTermNum(int maxTermNum) {
		this.maxTermNum = maxTermNum;
		return this;
	}

	public int getMaxSegNum() {
		return maxSegNum;
	}

	public TestDataGeneratorBuilder setMaxSegNum(int maxSegNum) {
		this.maxSegNum = maxSegNum;
		return this;
	}

	public int getMaxTerm() {
		return maxTerm;
	}

	public TestDataGeneratorBuilder setMaxTerm(int maxTerm) {
		this.maxTerm = maxTerm;
		return this;
	}

	public int getMaxMidNum() {
		return maxMidNum;
	}

	public TestDataGeneratorBuilder setMaxMidNum(int maxMidNum) {
		this.maxMidNum = maxMidNum;
		return this;
	}

	public int getMaxTime() {
		return maxTime;
	}

	public TestDataGeneratorBuilder setMaxTime(int maxTime) {
		this.maxTime = maxTime;
		return this;
	}

	public int getMaxIntervalLen() {
		return maxIntervalLen;
	}

	public TestDataGeneratorBuilder setMaxIntervalLen(int maxIntervalLen) {
		this.maxIntervalLen = maxIntervalLen;
		return this;
	}

	public int getMaxCount() {
		return maxCount;
	}

	public TestDataGeneratorBuilder setMaxCount(int maxCount) {
		this.maxCount = maxCount;
		return this;
	}

}
