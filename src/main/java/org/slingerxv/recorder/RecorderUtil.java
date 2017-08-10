/*
 * Copyright (c) 2016-present The Recorder Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
	private static final String PRIMARY_KEY = "`pk_id`";
	private static ConcurrentHashMap<Class<? extends IRecorder>, List<Field>> logFieldCache = new ConcurrentHashMap<>();
	private static Map<SQLType, Set<SQLType>> CHANGE_ALLOW_MAP = new HashMap<>();

	private RecorderUtil() {
	}

	static {
		// bigint可变动列表
		Set<SQLType> bigintlist = new HashSet<>();
		CHANGE_ALLOW_MAP.put(SQLType.BIGINT, bigintlist);
		bigintlist.add(SQLType.VARCHAR);
		bigintlist.add(SQLType.LONGTEXT);
		bigintlist.add(SQLType.TEXT);
		bigintlist.add(SQLType.BIGINT);
		// bit可变动列表
		Set<SQLType> bitlist = new HashSet<>();
		CHANGE_ALLOW_MAP.put(SQLType.BIT, bitlist);
		bitlist.add(SQLType.LONGTEXT);
		bitlist.add(SQLType.VARCHAR);
		bitlist.add(SQLType.TEXT);
		bitlist.add(SQLType.BIGINT);
		bitlist.add(SQLType.INTEGER);
		bitlist.add(SQLType.INT);
		bitlist.add(SQLType.BIT);
		// int可变动列表
		Set<SQLType> intlist = new HashSet<>();
		CHANGE_ALLOW_MAP.put(SQLType.INT, intlist);
		CHANGE_ALLOW_MAP.put(SQLType.INTEGER, intlist);
		intlist.add(SQLType.LONGTEXT);
		intlist.add(SQLType.VARCHAR);
		intlist.add(SQLType.TEXT);
		intlist.add(SQLType.BIGINT);
		intlist.add(SQLType.INTEGER);
		intlist.add(SQLType.INT);
		// short可变动列表
		Set<SQLType> shortlist = new HashSet<>();
		CHANGE_ALLOW_MAP.put(SQLType.TINYINT, shortlist);
		shortlist.add(SQLType.LONGTEXT);
		shortlist.add(SQLType.VARCHAR);
		shortlist.add(SQLType.TEXT);
		shortlist.add(SQLType.BIGINT);
		shortlist.add(SQLType.INT);
		shortlist.add(SQLType.INTEGER);
		shortlist.add(SQLType.TINYINT);
		// varchar变动列表
		Set<SQLType> varcharlist = new HashSet<>();
		varcharlist.add(SQLType.LONGTEXT);
		varcharlist.add(SQLType.VARCHAR);
		varcharlist.add(SQLType.TEXT);
		varcharlist.add(SQLType.INT);
		varcharlist.add(SQLType.BIGINT);
		CHANGE_ALLOW_MAP.put(SQLType.VARCHAR, varcharlist);
		// text变动列表
		Set<SQLType> text = new HashSet<>();
		CHANGE_ALLOW_MAP.put(SQLType.TEXT, text);
		text.add(SQLType.LONGTEXT);
		text.add(SQLType.TEXT);
		text.add(SQLType.VARCHAR);
		// longtext变动列表
		Set<SQLType> longtextlist = new HashSet<>();
		CHANGE_ALLOW_MAP.put(SQLType.LONGTEXT, longtextlist);
		longtextlist.add(SQLType.LONGTEXT);

	}

	/**
	 * 构建表是否存在检测语句
	 * 
	 * @param tableName
	 *            数据库表名
	 * @return
	 */
	public static String buildExistTableSqlMYSQL(String tableName) {
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
	 * @throws RecorderQueryBuilderException
	 */
	public static String buildSelectCountTableSqlMYSQL(RecorderQueryBuilder builder)
			throws RecorderQueryBuilderException {
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
	 * @throws RecorderQueryBuilderException
	 */
	public static String buildSelectTableSqlMYSQL(RecorderQueryBuilder builder) throws RecorderQueryBuilderException {
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
	public static String buildCreateTableSqlMYSQL(IRecorder alog, String dbEngine, String charset)
			throws UnsupportedEncodingException {
		StringBuilder createTableBuffer = new StringBuilder();
		String tableName = getLogTableName(alog, System.currentTimeMillis());
		List<Field> fieldAccessV2 = getLogFields(alog.getClass());
		createTableBuffer.append("create table if not exists ").append(tableName).append(" (").append(line());
		createTableBuffer.append(PRIMARY_KEY + " int primary key not null auto_increment");
		for (Field field : fieldAccessV2) {
			Col annotation = field.getAnnotation(Col.class);
			if (annotation == null) {
				continue;
			}
			SQLType type = annotation.type();
			int size = annotation.size();
			if (type == SQLType.VARCHAR && size <= 0) {
				size = 255;
			}
			String sizeStr = size > 0 ? "(" + size + ")" : "";
			String comment = annotation.comment();
			String tableFieldName = "`" + field.getName() + "`";
			createTableBuffer.append(",").append(line()).append(tableFieldName).append(" ").append(type.name())
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
			info.setType(SQLType.valueOf(columns.getString("TYPE_NAME")));
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
	public static String buildInsertTableSqlMYSQL(IRecorder alog)
			throws IllegalArgumentException, IllegalAccessException {
		StringBuilder fieldBuffer = new StringBuilder();
		StringBuilder valueBuffer = new StringBuilder();
		String tableName = getLogTableName(alog, System.currentTimeMillis());
		List<Field> fieldAccessV2 = getLogFields(alog.getClass());
		for (Field field : fieldAccessV2) {
			fieldBuffer.append("`" + field.getName() + "`").append(",");
			valueBuffer.append("'" + field.get(alog) + "'").append(",");
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
	public static String buildColumnIncreaseSqlMYSQL(final String tableName, final String fieldName, final SQLType type,
			final int size, final String comment) {
		final String sql = "alter table `" + tableName + "` add column `" + fieldName + "` " + type.name()
				+ (size > 0 ? "(" + size + ")" : "varchar".equalsIgnoreCase(type.name()) ? "(255)" : "") + " comment '"
				+ comment + "';";
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
	public static String buildColumnDecreaseSqlMYSQL(String tableName, String fieldName) {
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
	public static String buildColumnModifySqlMYSQL(final String tableName, final String fieldName, final SQLType type,
			final int size, final String comment) {
		final String sql = "alter table `" + tableName + "` modify column `" + fieldName + "` " + type.name()
				+ (size > 0 ? "(" + size + ")" : "varchar".equalsIgnoreCase(type.name()) ? "(255)" : "") + " comment '"
				+ comment + "';";
		log.debug(sql);
		return sql;
	}

	public static boolean isSame(ColumnInfo now, ColumnInfo old) {
		if (((now.getType().equals(SQLType.INT)) || (now.getType().equals(SQLType.INTEGER))
				|| (now.getType().name().startsWith(SQLType.INT.name())))
				&& ((old.getType().equals(SQLType.INTEGER)) || (old.getType().equals(SQLType.INT))
						|| (old.getType().name().startsWith(SQLType.INT.name())))) {
			return true;
		}

		if (now.getType().equals(SQLType.BIGINT) && old.getType().equals(now.getType())) {
			return true;
		}
		if (now.getType().equals(SQLType.TEXT) && old.getType().equals(now.getType())) {
			return true;
		}
		if (now.getType().equals(SQLType.LONGTEXT) && old.getType().equals(now.getType())) {
			return true;
		}
		if (now.getType().equals(SQLType.BIT) && old.getType().equals(now.getType())) {
			return true;
		}
		if (now.getType().equals(SQLType.TINYINT) && old.getType().equals(now.getType())) {
			return true;
		}
		return now.getType().equals(old.getType()) && now.getSize() <= old.getSize();
	}

	public static boolean ableChange(final ColumnInfo info, final ColumnInfo info2) {
		final Set<SQLType> set = CHANGE_ALLOW_MAP.get(info.getType());
		if (set == null) {
			return false;
		}
		return set.contains(info2.getType());
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
	public static List<String[]> getLogHeader(final Class<? extends IRecorder> clss) {
		List<String[]> result = new ArrayList<>();
		List<Field> fieldAccessV2 = getLogFields(clss);
		for (Field field : fieldAccessV2) {
			Col annotation = field.getAnnotation(Col.class);
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
	public static List<String> getTableNames(final Connection conn) throws SQLException {
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
		RollType logRollType = alog.rollType();
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
			RollType logRollType = newInstance.rollType();
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
			return field.getAnnotation(Col.class) != null;
		});
		logFieldCache.put(logClass, fields);
		return fields;
	}

}
