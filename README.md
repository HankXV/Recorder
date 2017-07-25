[![Build Status](https://travis-ci.org/HankXV/Recorder.svg?branch=master)](https://travis-ci.org/HankXV/Recorder)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/8558598883684247a0e568b7ad30bf4d)](https://www.codacy.com/app/104381832/Recorder?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=HankXV/Recorder&amp;utm_campaign=Badge_Grade)
# Brief Introduction
A framework that helps you log in MySQL\MariaDB<br>
![](/recorder-thumb.png)
## Environment
Jdk8 or above<br>
mysql-connector-java-5.x
# Quick Start
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
