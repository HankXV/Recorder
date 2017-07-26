package org.slingerxv.recorder;

import java.io.IOException;
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
	 * @throws RecorderCheckException
	 */
	public void registTable(Class<? extends IRecorder> bean) throws RecorderCheckException {
		String lowerCase = bean.getSimpleName().toLowerCase();
		if (tables.containsKey(lowerCase)) {
			throw new RecorderCheckException("table name: " + lowerCase + " duplicated!");
		}
		tables.put(lowerCase, bean);
	}

	/**
	 * 扫描包内AbstractLog的类型
	 * 
	 * @see TimeBasedLog
	 * @param packageName
	 * @throws RecorderCheckException
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	@SuppressWarnings("unchecked")
	public void registTable(String packageName) throws RecorderCheckException, ClassNotFoundException, IOException {
		List<Class<?>> classes = new ArrayList<>();
		classes = ReflectionUtil.getClassesByPackage(packageName, IRecorder.class);
		log.debug("package：{}，scan classes：{}。", packageName, classes.size());
		for (Class<?> temp : classes) {
			registTable((Class<? extends IRecorder>) temp);
		}
	}

	/**
	 * 开始执行检查
	 * 
	 * @param con
	 * @throws SQLException
	 * @throws RecorderCheckException
	 */
	public void executeCheck(Connection con) throws SQLException, RecorderCheckException {
		log.info("start check all recorders...");
		for (Class<? extends IRecorder> clss : tables.values()) {
			executeCheck(con, clss);
		}
		log.info("check all recorders done。");
	}

	private void executeCheck(Connection con, Class<? extends IRecorder> clss)
			throws SQLException, RecorderCheckException {
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
			log.info("start check：" + logTableName);
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
					throw new RecorderCheckException("recorder's field：" + field.getName() + " must be public!");
				}
				String tableFieldName = field.getName();
				ColumnInfo info = new ColumnInfo();
				info.setTableFieldName(tableFieldName);
				info.setType(annotation.type());
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
							throw new RecorderCheckException("unable to change column,table：" + logTableName + ",new:"
									+ info + ",old:" + source);
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
					if (field.getAnnotation(Column.class) != null && field.getName().equals(info.getTableFieldName())) {
						contains = true;
						break;
					}
				}
				if (!contains) {
					decreaseList.add(info.getTableFieldName());
				}
			}

			for (ColumnInfo col : increaseList) {
				try (PreparedStatement prepareStatement = con.prepareStatement(RecorderUtil.buildColumnIncreaseSqlMYSQL(
						logTableName, col.getTableFieldName(), col.getType(), col.getSize(), col.getComment()));) {
					if (prepareStatement.executeUpdate() == 0) {
						log.error("add column failed，logger:" + logTableName + "-----column:" + col.getTableFieldName()
								+ " " + col.getType() + " " + col.getSize());
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
						.prepareStatement(RecorderUtil.buildColumnDecreaseSqlMYSQL(logTableName, colName));) {
					if (prepareStatement.executeUpdate() == 0) {
						log.error("delete column failed，logger:" + logTableName + "-----column:" + colName);
					} else {
						log.info("delete column success，logger:" + logTableName + "-----column:" + colName);
					}
				} catch (Exception e) {
					log.error(e, e);
				}

			}
			for (ColumnInfo col : modifyList) {
				try (PreparedStatement prepareStatement = con.prepareStatement(RecorderUtil.buildColumnModifySqlMYSQL(
						logTableName, col.getTableFieldName(), col.getType(), col.getSize(), col.getComment()));) {
					if (prepareStatement.executeUpdate() == 0) {
						log.error("change column failed,logger:" + logTableName + "-----column:"
								+ col.getTableFieldName() + " " + col.getType() + " " + col.getSize());
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
