package org.slingerxv.recorder;

public class UserLog extends TimeBasedLog {
	@Column(type = SQLType.VARCHAR, size = 255, comment = "user name")
	public String name;
	@Column(comment = "user age")
	public int age;
	@Column(type = SQLType.VARCHAR, size = 255, comment = "user address")
	public String address;

	@Override
	public RollType rollType() {
		return RollType.DAY_ROLL;
	}
}
