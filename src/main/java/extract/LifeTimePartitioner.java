package extract;

public class LifeTimePartitioner {
	public static int part(long width) {
		return (int) (Math.log(width) / Math.log(2));
	}
}
