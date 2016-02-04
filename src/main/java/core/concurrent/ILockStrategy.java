package core.concurrent;

import java.util.List;

/**
 * 对枷锁机制进行封装
 * 
 * @author xiafan
 *
 */
public interface ILockStrategy {

	public void readPrepare(List<String> keywords);

	public void writePrepare(List<String> keywords);

	public void read(String keyword);

	public void readOver(String keyword);

	public void write(String keyword);

	public void writeOver(String keyword);

	public void cleanup();
}
