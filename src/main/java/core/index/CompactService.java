package core.index;

/**
 * to avoid scan disk to find those posting lists needing compaction, a lazy policy is adopted. 
 * Only posting list that has come into memory will be checked for compaction
 * @author xiafan
 *
 */
public class CompactService {

}
