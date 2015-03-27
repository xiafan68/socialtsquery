package core.index;

/**
 * 
 * @author xiafan
 * 
 */
public class PartitionMeta {
	public int partIdx;// lifetime的上界

	public PartitionMeta(int partIdx) {
		super();
		this.partIdx = partIdx;
	}

	public int getLifetimeBound() {
		return (int) Math.pow(2, partIdx) - 1;
	}

	@Override
	public boolean equals(Object meta) {
		return partIdx == ((PartitionMeta) meta).partIdx;
	}

	@Override
	public String toString() {
		return "PartitionMeta [partIdx=" + partIdx + "]";
	}
}
