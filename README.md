[![](https://img.shields.io/badge/maven-v2.0--alpha-green.svg)](https://mvnrepository.com/artifact/org.slingerxv/recorder)
[![](https://img.shields.io/badge/license-Apache%202-green.svg)](http://www.apache.org/licenses/LICENSE-2.0)
# 什么是Recorder？
主要用于在游戏中**记录日志**或者流水(比如货币的花销流水，任务的完成日志)，方便后台统计和查询。此框架目前只适用于数据库 **MySQL\MariaDB**。<br>
![](/recorder-thumb.png)
## 环境要求
Jdk8 or above<br>
mysql-connector-java-5.x
# 快速开始
### Maven
	<dependency>
	    <groupId>org.slingerxv</groupId>
	    <artifactId>recorder</artifactId>
	    <version>2.0-alpha</version>
	</dependency>
### Gradle
	compile 'org.slingerxv:recorder:2.0-alpha'
	
建立一个日志的Bean

```java
	public class UserLog extends TimeBasedLog {
		@Col(type = SQLType.VARCHAR, size = 255, comment = "user name")
		public String name;
		@Col(comment = "user age")
		public int age;
		@Col(type = SQLType.VARCHAR, size = 255, comment = "user address")
		public String address;
	
		@Override
		public RollType rollType() {
			return RollType.DAY_ROLL;
		}
	}
```
初始化日志代理，并执行写入任务，完成！

```java

	UserLog userLog = new UserLog();
	userLog.name="HankXV";
	userLog.age=101;
	userLog.address="home";
	new RecorderProxy
	.RecorderProxyBuilder()
	.dataSource(yourDatasource)
	.build()
	.startServer()
	.execute(userLog);
		
```
# 更新日志
## v2.0-release
	1.表检查器支持多表同Bean的检查(垂直分表)，表名逗号隔开
## v2.0-alpha
	此版本为pre-release版本
	1.减少了很多复杂的命名
	2.日志Bean的字段从私有变为公有，操作更方便
	3.增加查询日志的API
	4.简化SQLType的名称
