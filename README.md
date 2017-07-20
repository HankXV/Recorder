[![Build Status](https://travis-ci.org/HankXV/Recorder.svg?branch=master)](https://travis-ci.org/HankXV/Recorder)
# Brief Introduction
A framework that helps you log in MySQL
## Environment
Jdk8 or above<br>
mysql-connector-java-5.x
# Quick start
```java
	public class UserLog extends TimeBasedLog {
		@Column(type = SQLType.MYSQL_varchar, size = 255, comment = "user name")
		public String name;
		@Column(comment = "user age")
		public int age;
		@Column(type = SQLType.MYSQL_varchar, size = 255, comment = "user address")
		public String address;
	
		@Override
		public RollType getLogRollType() {
			return RollType.DAY_ROLL;
		}
	}
```
```java

	UserLog userLog = new UserLog();
	userLog.name="HankXV";
	userLog.age=101;
	userLog.address="home";
	new RecorderProxy.RecorderProxyBuilder().dataSource(datasource).build().execute(userLog);
		
```