package util;

public enum ProfileField {
	TOTAL_TIME, UPDATE_CAND, // 更新当前cand的状态
	MAINTAIN_CAND, // 更新所有cand对象的状态

	// io related fields
	READ_BLOCK, // time
	NUM_BLOCK,

	// fields for # of records
	TOPK, CAND, WASTED_REC,

	LEVELS, HITTED_LEVELS, HITTED_LEVEL_NUM
}
