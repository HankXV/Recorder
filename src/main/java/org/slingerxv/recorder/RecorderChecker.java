package org.slingerxv.recorder;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * 日志表结构变动检查器
 * 
 * @author hank
 *
 */
public class RecorderChecker {
	private static Logger log = LogManager.getLogger();
	private HashMap<String, Class<? extends IRecorder>> tables = new HashMap<>();
	

	public void clearTables() {
		tables.clear();
	}

	/**
	 * 注册一个bean，table名称默认为bean的简单名称小写
	 * 
	 * @param bean
	 *            JavaBean
	 * @throws Exception
	 */
	public void registTable(Class<? extends IRecorder> bean) throws Exception {
		String lowerCase = bean.getSimpleName().toLowerCase();
		if (tables.containsKey(lowerCase)) {
			throw new Exception("table name: " + lowerCase + " duplicated!");
		}
		tables.put(lowerCase, bean);
	}

	/**
	 * 扫描包内AbstractLog的类型
	 * 
	 * @see TimeBasedLog
	 * @param packageName
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public void registTable(String packageName) throws Exception {
		List<Class<?>> classes = new ArrayList<>();
		try {
			classes = ReflectionUtil.getClassesByPackage(packageName, IRecorder.class);
		} catch (Exception e) {
			log.error(e, e);
		}
		log.debug("包：{}，共扫描类：{}个。", packageName, classes.size());
		for (Class<?> temp : classes) {
			registTable((Class<? extends IRecorder>) temp);
		}
	}

	/**
	 * 开始执行检查
	 * 
	 * @param con
	 * @throws Exception
	 */
	public void executeCheck(Connection con) throws Exception {
		log.info("开始检查所有日志表结构...");
		for (Class<? extends IRecorder> clss : tables.values()) {
			executeCheck(con, clss);
		}
		log.info("检查所有日志表结构完毕。");
	}

	private void executeCheck(Connection con, Class<? extends IRecorder> clss) throws Exception {
		// if (!AbstractLog.class.isAssignableFrom(clss)) {
		// return;
		// }
		// 是否是虚拟类
		if (Modifier.isAbstract(clss.getModifiers())) {
			return;
		}
		// if (clss.isInterface()) {
		// return;
		// }
		List<String> tableNames = RecorderUtil.getTableNames(con);
		for (String logTableName : tableNames) {
			if (!logTableName.startsWith(clss.getSimpleName().toLowerCase())) {
				continue;
			}
			log.info("开始检查表结构：" + logTableName);
			TableInfo columnDefine = RecorderUtil.getColumnDefine(con, logTableName);
			List<ColumnInfo> increaseList = new ArrayList<>();
			List<String> decreaseList = new ArrayList<>();
			List<ColumnInfo> modifyList = new ArrayList<>();
			// 检查增加字段
			List<Field> logFields = RecorderUtil.getLogFields(clss);
			for (Field field : logFields) {
				Column annotation = field.getAnnotation(Column.class);
				if (annotation == null) {
					continue;
				}
				// 这里要检查一下字段是否是公共的，因为ReflectASM只能反射public的字段
				if (!Modifier.isPublic(field.getModifiers())) {
					throw new Exception("日志字段：" + field.getName() + "必须为公共字段!");
				}
				String tableFieldName = field.getName();
				ColumnInfo info = new ColumnInfo();
				info.setTableFieldName(tableFieldName);
				info.setType(annotation.type().getValue());
				info.setSize(annotation.size());
				info.setComment(annotation.comment());
				if (!columnDefine.getColumnInfos().containsKey(tableFieldName)) {
					increaseList.add(info);
				} else {
					// 检查变更字段
					ColumnInfo source = columnDefine.getColumnInfos().get(tableFieldName);
					if (!RecorderUtil.isSame(info, source)) {
						if (RecorderUtil.ableChange(info, source)) {
							modifyList.add(info);
						} else {
							throw new Exception("检测到变动但不允许变动,表名：" + logTableName + ",new:" + info + ",old:" + source);
						}
					}
				}
			}
			// 检查删除字段
			for (ColumnInfo info : columnDefine.getColumnInfos().values()) {
				if (columnDefine.getPrimaryKeys().contains(info.getTableFieldName())) {
					continue;
				}
				boolean contains = false;
				for (Field field : logFields) {
					if (field.getAnnotation(Column.class) != null
							&& field.getName().equals(info.getTableFieldName())) {
						contains = true;
						break;
					}
				}
				if (!contains) {
					decreaseList.add(info.getTableFieldName());
				}
			}

			for (ColumnInfo col : increaseList) {
				try (PreparedStatement prepareStatement = con.prepareStatement(RecorderUtil.buildColumnIncreaseSql_MYSQL(
						logTableName, col.getTableFieldName(), col.getType(), col.getSize(), col.getComment()));) {
					if (prepareStatement.executeUpdate() == 0) {
						SQLException sqlException = new SQLException("add column failed，logger:" + logTableName
								+ "-----column:" + col.getTableFieldName() + " " + col.getType() + " " + col.getSize());
						log.error(sqlException, sqlException);
					} else {
						log.info("add column success，logger:" + logTableName + "-----column:" + col.getTableFieldName()
								+ " " + col.getType() + " " + col.getSize());
					}
				} catch (Exception e) {
					log.error(e, e);
				}
			}
			for (String colName : decreaseList) {
				try (PreparedStatement prepareStatement = con
						.prepareStatement(RecorderUtil.buildColumnDecreaseSql_MYSQL(logTableName, colName));) {
					if (prepareStatement.executeUpdate() == 0) {
						SQLException sqlException = new SQLException(
								"delete column failed，logger:" + logTableName + "-----column:" + colName);
						log.error(sqlException, sqlException);
					} else {
						log.info("delete column success，logger:" + logTableName + "-----column:" + colName);
					}
				} catch (Exception e) {
					log.error(e, e);
				}

			}
			for (ColumnInfo col : modifyList) {
				try (PreparedStatement prepareStatement = con.prepareStatement(RecorderUtil.buildColumnModifySql_MYSQL(
						logTableName, col.getTableFieldName(), col.getType(), col.getSize(), col.getComment()));) {
					if (prepareStatement.executeUpdate() == 0) {
						SQLException sqlException = new SQLException("change column failed,logger:" + logTableName
								+ "-----column:" + col.getTableFieldName() + " " + col.getType() + " " + col.getSize());
						log.error(sqlException, sqlException);
					} else {
						log.info("change column success,logger:" + logTableName + "-----column:"
								+ col.getTableFieldName() + " " + col.getType() + " " + col.getSize());
					}
				} catch (Exception e) {
					log.error(e, e);
				}
			}
			log.info("check recorder logger:" + logTableName + "done！");
		}
	}


	public Class<? extends IRecorder> getTableClass(String tableName) {
		return tables.get(tableName);
	}
}
