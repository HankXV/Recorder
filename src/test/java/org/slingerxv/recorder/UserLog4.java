package org.slingerxv.recorder;

public class UserLog4 extends TimeBasedLog {
	@Col(type = SQLType.VARCHAR, size = 255, comment = "user name")
	public String name;
	@Col(comment = "user age")
	public int age;
	@Col(type = SQLType.VARCHAR, size = 255, comment = "user address")
	public String address;

	@Override
	public RollType rollType() {
		return RollType.NEVER_ROLL;
	}
}
