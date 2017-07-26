package org.slingerxv.recorder;


/**
 * 基础日志
 * 
 * @author hank
 *
 */
public abstract class TimeBasedLog implements IRecorder {
	@Column(type = SQLType.BIGINT, comment = "create time")
	public long createTime = System.currentTimeMillis();
}
