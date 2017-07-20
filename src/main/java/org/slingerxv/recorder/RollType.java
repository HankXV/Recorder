package org.slingerxv.recorder;

/**
 * 日志滚动类型
 * 
 * @author hank
 *
 */
public enum RollType {
	/**
	 * 日表
	 */
	DAY_ROLL(1),
	/**
	 * 月表
	 */
	MONTH_ROLL(2),
	/**
	 * 年表
	 */
	YEAR_ROLL(3),
	/**
	 * 固定
	 */
	NEVER_ROLL(4);
	private int value;

	RollType(int value) {
		this.value = value;
	}

	public int getValue() {
		return this.value;
	}
}
