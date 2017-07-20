package org.slingerxv.recorder;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * 日志辅助类
 * 
 * @author hank
 *
 */
public class RecorderUtil {
	private static Logger log = LogManager.getLogger();
	private static String PRIMARY_KEY_FIELD_NAME = "`pk_id`";
	private static ConcurrentHashMap<Class<? extends IRecorder>, List<Field>> logFieldCache = new ConcurrentHashMap<>();

	private static Map<SQLType, Set<SQLType>> CHANGE_ALLOW_MAP = new HashMap<>();
	static {
		// bigint可变动列表
		Set<SQLType> bigintlist = new HashSet<>();
		CHANGE_ALLOW_MAP.put(SQLType.MYSQL_bigint, bigintlist);
		bigintlist.add(SQLType.MYSQL_varchar);
		bigintlist.add(SQLType.MYSQL_longtext);
		bigintlist.add(SQLType.MYSQL_text);
		bigintlist.add(SQLType.MYSQL_bigint);
		// bit可变动列表
		Set<SQLType> bitlist = new HashSet<>();
		CHANGE_ALLOW_MAP.put(SQLType.MYSQL_bit, bitlist);
		bitlist.add(SQLType.MYSQL_longtext);
		bitlist.add(SQLType.MYSQL_varchar);
		bitlist.add(SQLType.MYSQL_text);
		bitlist.add(SQLType.MYSQL_bigint);
		bitlist.add(SQLType.MYSQL_integer);
		bitlist.add(SQLType.MYSQL_int);
		bitlist.add(SQLType.MYSQL_bit);
		// int可变动列表
		Set<SQLType> intlist = new HashSet<>();
		CHANGE_ALLOW_MAP.put(SQLType.MYSQL_int, intlist);
		CHANGE_ALLOW_MAP.put(SQLType.MYSQL_integer, intlist);
		intlist.add(SQLType.MYSQL_longtext);
		intlist.add(SQLType.MYSQL_varchar);
		intlist.add(SQLType.MYSQL_text);
		intlist.add(SQLType.MYSQL_bigint);
		intlist.add(SQLType.MYSQL_integer);
		intlist.add(SQLType.MYSQL_int);
		// short可变动列表
		Set<SQLType> shortlist = new HashSet<>();
		CHANGE_ALLOW_MAP.put(SQLType.MYSQL_tinyint, shortlist);
		shortlist.add(SQLType.MYSQL_longtext);
		shortlist.add(SQLType.MYSQL_varchar);
		shortlist.add(SQLType.MYSQL_text);
		shortlist.add(SQLType.MYSQL_bigint);
		shortlist.add(SQLType.MYSQL_int);
		shortlist.add(SQLType.MYSQL_integer);
		shortlist.add(SQLType.MYSQL_tinyint);
		// varchar变动列表
		Set<SQLType> varcharlist = new HashSet<>();
		varcharlist.add(SQLType.MYSQL_longtext);
		varcharlist.add(SQLType.MYSQL_varchar);
		varcharlist.add(SQLType.MYSQL_text);
		varcharlist.add(SQLType.MYSQL_int);
		varcharlist.add(SQLType.MYSQL_bigint);
		CHANGE_ALLOW_MAP.put(SQLType.MYSQL_varchar, varcharlist);
		// text变动列表
		Set<SQLType> text = new HashSet<>();
		CHANGE_ALLOW_MAP.put(SQLType.MYSQL_text, text);
		text.add(SQLType.MYSQL_longtext);
		text.add(SQLType.MYSQL_text);
		text.add(SQLType.MYSQL_varchar);
		// longtext变动列表
		Set<SQLType> longtextlist = new HashSet<>();
		CHANGE_ALLOW_MAP.put(SQLType.MYSQL_longtext, longtextlist);
		longtextlist.add(SQLType.MYSQL_longtext);

	}

	/**
	 * 构建表是否存在检测语句
	 * 
	 * @param tableName
	 *            数据库表名
	 * @return
	 */
	public static String buildExistTableSql_MYSQL(String tableName) {
		String sql = "SHOW TABLES  LIKE '" + tableName + "'";
		log.debug(sql);
		return sql;
	}

	/**
	 * 构建查找数量SQL
	 * 
	 * @param builder
	 *            构造器
	 * @return sql语句
	 * @throws Exception
	 */
	public static String buildSelectCountTableSql_MYSQL(RecorderQueryBuilder builder) throws Exception {
		String build = builder.build();
		log.debug(build);
		return build;
	}

	/**
	 * 构建表查询语句
	 * 
	 * @param tableName
	 * @param start
	 * @param end
	 * @return
	 * @throws Exception
	 */
	public static String buildSelectTableSql_MYSQL(RecorderQueryBuilder builder) throws Exception {
		String build = builder.build();
		log.debug(build);
		return build;
	}

	/**
	 * 创建建表Sql
	 * 
	 * @param alog
	 * @return
	 * @throws UnsupportedEncodingException
	 */
	public static String buildCreateTableSql_MYSQL(IRecorder alog, String dbEngine, String charset)
			throws UnsupportedEncodingException {
		StringBuilder createTableBuffer = new StringBuilder();
		String tableName = getLogTableName(alog, System.currentTimeMillis());
		List<Field> fieldAccessV2 = getLogFields(alog.getClass());
		createTableBuffer.append("create table if not exists ").append(tableName).append(" (").append(line());
		createTableBuffer.append(PRIMARY_KEY_FIELD_NAME + " int primary key not null auto_increment");
		for (Field field : fieldAccessV2) {
			Column annotation = field.getAnnotation(Column.class);
			if (annotation == null) {
				continue;
			}
			SQLType type = annotation.type();
			int size = annotation.size();
			if (type == SQLType.MYSQL_varchar) {
				if (size <= 0) {
					size = 255;
				}
			}
			String sqlType = type.getValue();
			String sizeStr = size > 0 ? "(" + size + ")" : "";
			String comment = annotation.comment();
			String tableFieldName = "`" + field.getName() + "`";
			createTableBuffer.append(",").append(line()).append(tableFieldName).append(" ").append(sqlType)
					.append(sizeStr).append(" null comment ").append("'").append(comment).append("'");
		}
		createTableBuffer.append(")");
		createTableBuffer.append("engine=" + dbEngine + " auto_increment=1 default charset=" + charset + " comment '")
				.append(alog.getClass().getSimpleName()).append("'");
		String sql = createTableBuffer.toString();
		log.debug(sql);
		return sql;
	}

	/**
	 * 从数据库获取列定义
	 * 
	 * @param conn
	 *            数据库链接
	 * @param tableName
	 *            数据库表名
	 * @return 表信息
	 * @throws SQLException
	 */
	public static TableInfo getColumnDefine(Connection conn, String tableName) throws SQLException {
		TableInfo tableInfo = new TableInfo();
		DatabaseMetaData metaData = conn.getMetaData();
		ResultSet columns = metaData.getColumns(null, "%", tableName, "%");
		ResultSet primaryKey = metaData.getPrimaryKeys(null, "%", tableName);
		while (primaryKey.next()) {
			tableInfo.getPrimaryKeys().add(primaryKey.getString(4));
		}
		while (columns.next()) {
			ColumnInfo info = new ColumnInfo();
			info.setTableFieldName(columns.getString("COLUMN_NAME"));
			info.setType(columns.getString("TYPE_NAME").toLowerCase());
			info.setSize(columns.getInt("COLUMN_SIZE"));
			info.setNullable(columns.getBoolean("IS_NULLABLE"));
			tableInfo.getColumnInfos().put(info.getTableFieldName(), info);
		}
		return tableInfo;
	}

	/**
	 * 创建插入Sql
	 * 
	 * @param aLog
	 * @return
	 * @throws IllegalAccessException
	 * @throws IllegalArgumentException
	 */
	public static String buildInsertTableSql_MYSQL(IRecorder alog)
			throws IllegalArgumentException, IllegalAccessException {
		StringBuilder fieldBuffer = new StringBuilder();
		StringBuilder valueBuffer = new StringBuilder();
		String tableName = getLogTableName(alog, System.currentTimeMillis());
		List<Field> fieldAccessV2 = getLogFields(alog.getClass());
		for (Field field : fieldAccessV2) {
			String tableFieldName = "`" + field.getName() + "`";
			Object object = field.get(alog);
			String parseFieldValueType = object.toString();
			fieldBuffer.append(tableFieldName).append(",");
			valueBuffer.append(parseFieldValueType).append(",");
		}
		fieldBuffer.deleteCharAt(fieldBuffer.length() - 1);
		valueBuffer.deleteCharAt(valueBuffer.length() - 1);
		StringBuilder insertTableBuffer = new StringBuilder();
		insertTableBuffer.append("insert into `").append(tableName).append("`(").append(fieldBuffer)
				.append(") values (").append(valueBuffer).append(")");
		String sql = insertTableBuffer.toString();
		log.debug(sql);
		return sql;
	}

	/**
	 * 创建列增加Sql
	 * 
	 * @param tableName
	 * @param fieldNameAndType
	 * @return
	 */
	public static String buildColumnIncreaseSql_MYSQL(String tableName, String fieldName, String type, int size,
			String comment) {
		String sql = "alter table `" + tableName + "` add column `" + fieldName + "` " + type
				+ (size > 0 ? "(" + size + ")" : type.equals("varchar") ? "(255)" : "") + " comment '" + comment + "';";
		log.debug(sql);
		return sql;
	}

	/**
	 * 创建列删除Sql
	 * 
	 * @param tableName
	 * @param fieldName
	 * @return
	 */
	public static String buildColumnDecreaseSql_MYSQL(String tableName, String fieldName) {
		String sql = "alter table `" + tableName + "` drop column `" + fieldName + "`;";
		log.debug(sql);
		return sql;
	}

	/**
	 * 创建列更改Sql
	 * 
	 * @param tableName
	 * @param fieldNameAndType
	 * @return
	 */
	public static String buildColumnModifySql_MYSQL(String tableName, String fieldName, String type, int size,
			String comment) {
		String sql = "alter table `" + tableName + "` modify column `" + fieldName + "` " + type
				+ (size > 0 ? "(" + size + ")" : type.equals("varchar") ? "(255)" : "") + " comment '" + comment + "';";
		log.debug(sql);
		return sql;
	}

	public static boolean isSame(ColumnInfo now, ColumnInfo old) {
		if ((((now.getType().equals(SQLType.MYSQL_int.getValue()))
				|| (now.getType().equals(SQLType.MYSQL_integer.getValue()))
				|| (now.getType().startsWith(SQLType.MYSQL_int.getValue()))))
				&& (((old.getType().equals(SQLType.MYSQL_integer.getValue()))
						|| (old.getType().equals(SQLType.MYSQL_int.getValue()))
						|| (old.getType().startsWith(SQLType.MYSQL_int.getValue()))))) {
			return true;
		}

		if ((now.getType().equals(SQLType.MYSQL_bigint.getValue())) && (old.getType().equals(now.getType()))) {
			return true;
		}
		if ((now.getType().equals(SQLType.MYSQL_text.getValue())) && (old.getType().equals(now.getType()))) {
			return true;
		}
		if ((now.getType().equals(SQLType.MYSQL_longtext.getValue())) && (old.getType().equals(now.getType()))) {
			return true;
		}
		if ((now.getType().equals(SQLType.MYSQL_bit.getValue())) && (old.getType().equals(now.getType()))) {
			return true;
		}
		if ((now.getType().equals(SQLType.MYSQL_tinyint.getValue())) && (old.getType().equals(now.getType()))) {
			return true;
		}
		return (now.getType().equals(old.getType())) && (now.getSize() <= old.getSize());
	}

	public static boolean ableChange(ColumnInfo info, ColumnInfo info2) {
		SQLType typeByValue = SQLType.getTypeByValue(info.getType());
		if (typeByValue == null) {
			return false;
		}
		Set<SQLType> set = CHANGE_ALLOW_MAP.get(typeByValue);
		if (set == null) {
			return false;
		}
		SQLType typeByValue2 = SQLType.getTypeByValue(info2.getType());
		return set.contains(typeByValue2);
	}

	private static String line() {
		return System.getProperty("line.separator");
	}

	/**
	 * 获取日志头信息
	 * 
	 * @param clss
	 *            日志类
	 * @return [字段名，字段解释]
	 */
	public static List<String[]> getLogHeader(Class<? extends IRecorder> clss) {
		List<String[]> result = new ArrayList<>();
		List<Field> fieldAccessV2 = getLogFields(clss);
		for (Field field : fieldAccessV2) {
			Column annotation = field.getAnnotation(Column.class);
			if (annotation == null) {
				continue;
			}
			String[] temp = new String[] { field.getName(), annotation.comment() };
			result.add(temp);
		}
		return result;
	}

	/**
	 * 从数据库获取表名
	 * 
	 * @param conn
	 *            数据库链接
	 * @return 数据库表名列表
	 * @throws SQLException
	 */
	public static List<String> getTableNames(Connection conn) throws SQLException {
		ResultSet tableRet = conn.getMetaData().getTables(null, "%", "%", null);
		List<String> tablenames = new ArrayList<String>();
		while (tableRet.next()) {
			tablenames.add(tableRet.getString("TABLE_NAME"));
		}
		return tablenames;
	}

	/**
	 * 获取此种日志当前带日期的名称
	 * 
	 * @param alog
	 * @return
	 */
	public static String getLogTableName(IRecorder alog, long millTime) {
		RollType logRollType = alog.getLogRollType();
		String tableName = alog.getClass().getSimpleName().toLowerCase();
		switch (logRollType) {
		case DAY_ROLL:
			tableName = tableName + new SimpleDateFormat("yyyyMMdd").format(new Date(millTime));
			break;
		case MONTH_ROLL:
			tableName = tableName + new SimpleDateFormat("yyyyMM").format(new Date(millTime));
			break;
		case YEAR_ROLL:
			tableName = tableName + new SimpleDateFormat("yyyy").format(new Date(millTime));
			break;
		case NEVER_ROLL:
			break;
		}
		return tableName;
	}

	/**
	 * 通过开始时间和结束时间查找相关表
	 * 
	 * @param alog
	 * @param start
	 * @param end
	 * @return
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	public static Set<String> getRelativeTableNames(Class<? extends IRecorder> alog, long start, long end)
			throws InstantiationException, IllegalAccessException {
		Calendar startCal = Calendar.getInstance(Locale.SIMPLIFIED_CHINESE);
		startCal.setTimeInMillis(start);
		startCal.set(Calendar.DAY_OF_MONTH, 1);
		startCal.clear(Calendar.HOUR_OF_DAY);
		startCal.clear(Calendar.MINUTE);
		startCal.clear(Calendar.SECOND);
		startCal.clear(Calendar.MILLISECOND);
		Set<String> result = new HashSet<>();
		IRecorder newInstance = alog.newInstance();
		do {
			String logTableName = getLogTableName(newInstance, startCal.getTimeInMillis());
			RollType logRollType = newInstance.getLogRollType();
			if (logRollType == RollType.DAY_ROLL) {
				if (startCal.getTimeInMillis() >= start) {
					result.add(logTableName);
				}
				startCal.add(Calendar.DAY_OF_YEAR, 1);
			} else if (logRollType == RollType.MONTH_ROLL) {
				result.add(logTableName);
				startCal.add(Calendar.MONTH, 1);
			} else if (logRollType == RollType.YEAR_ROLL) {
				result.add(logTableName);
				startCal.add(Calendar.YEAR, 1);
			} else if (logRollType == RollType.NEVER_ROLL) {
				result.add(logTableName);
				break;
			} else {
				break;
			}
		} while (startCal.getTimeInMillis() <= end);
		return result;
	}

	public static List<Field> getLogFields(Class<? extends IRecorder> logClass) {
		if (logFieldCache.containsKey(logClass)) {
			return logFieldCache.get(logClass);
		}
		List<Field> fields = ReflectionUtil.getFields(logClass, true, (field) -> {
			if (Modifier.isStatic(field.getModifiers())) {
				return false;
			}
			return field.getAnnotation(Column.class) != null;
		});
		logFieldCache.put(logClass, fields);
		return fields;
	}

}
