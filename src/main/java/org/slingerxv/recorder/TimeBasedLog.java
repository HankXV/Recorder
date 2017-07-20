package org.slingerxv.recorder;


/**
 * 基础日志
 * 
 * @author hank
 *
 */
public abstract class TimeBasedLog implements IRecorder {
	@Column(type = SQLType.MYSQL_bigint, comment = "记录时间")
	public long createTime = System.currentTimeMillis();
}
