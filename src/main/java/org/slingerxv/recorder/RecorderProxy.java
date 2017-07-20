package org.slingerxv.recorder;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

import javax.sql.DataSource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * 数据库日志记录服务器
 * 
 * @author hank
 *
 */
public class RecorderProxy {
	private static Logger log = LogManager.getLogger();
	private ThreadPoolExecutor threadPool;
	private BlockingQueue<Runnable> logTaskQueue;
	private RecorderChecker checker = new RecorderChecker();
	private boolean isStop = true;
	private LongAdder doneLogNum = new LongAdder();
	private LongAdder lostLogNum = new LongAdder();
	// -- config
	// 扫描项目包名(日志结构检查)
	private String[] scanPackages;
	// 任务线程池基本线程数
	private int threadCorePoolSize;
	// 任务线程池最大线程数
	private int threadMaximumPoolSize;
	// 任务上限数量
	private int taskMaxSize;
	// 数据库引擎
	private String dbEngine;
	// 编码
	private String charset;
	// 自定义线程池
	private ThreadPoolExecutor customInsertThreadPool;
	private Supplier<DataSource> dataSourceFactory;

	private RecorderProxy(RecorderProxyBuilder builder) {
		this.scanPackages = builder.scanPackages.toArray(new String[0]);
		this.taskMaxSize = builder.taskMaxSize;
		this.threadCorePoolSize = builder.threadCorePoolSize;
		this.threadMaximumPoolSize = builder.threadMaximumPoolSize;
		this.dbEngine = builder.dbEngine;
		this.charset = builder.charset;
		this.customInsertThreadPool = builder.customInsertThreadPool;
		if (builder.dataSourceFactory == null) {
			throw new NullPointerException("dataSourceFactory");
		}
		this.dataSourceFactory = builder.dataSourceFactory;
	}

	/**
	 * 执行一条日志记录的插入
	 * 
	 * @param baseLog
	 * @return
	 * @throws Exception
	 */
	public RecorderProxy execute(final IRecorder alog) throws Exception {
		if (isStop) {
			throw new Exception("server is stopped!");
		}

		if (alog != null) {
			if (getTaksCount() > taskMaxSize) {
				lostLogNum.increment();
				throw new Exception("task count is overload,drop task:" + alog);
			}
			threadPool.execute(() -> {
				try (Connection con = dataSourceFactory.get().getConnection();) {
					long now = System.currentTimeMillis();
					String buildExistTableSql_MYSQL = RecorderUtil
							.buildExistTableSql_MYSQL(RecorderUtil.getLogTableName(alog, now));
					try (PreparedStatement existStatement = con.prepareStatement(buildExistTableSql_MYSQL);
							ResultSet executeQuery = existStatement.executeQuery();) {
						if (!executeQuery.next()) {
							String buildCreateTableSql = RecorderUtil.buildCreateTableSql_MYSQL(alog, dbEngine,
									charset);
							try (PreparedStatement createStatement = con.prepareStatement(buildCreateTableSql);) {
								// 执行创建表
								createStatement.executeUpdate();
							}

						}
					}
					String buildInsertTableSql = RecorderUtil.buildInsertTableSql_MYSQL(alog);
					try (PreparedStatement insertStatement = con.prepareStatement(buildInsertTableSql);) {
						// 执行插入
						if (insertStatement.executeUpdate() > 0) {
							doneLogNum.increment();
						} else {
							log.error("log failed:" + alog);
							lostLogNum.increment();
						}
					}
				} catch (Exception e) {
					log.error(e, e);
					log.error("log failed:" + alog);
					lostLogNum.increment();
				}
			});
		}
		return this;
	}

	public Class<? extends IRecorder> getTableClassByName(String tableName) {
		return checker.getTableClass(tableName);
	}

	/**
	 * 查询日志条数
	 * 
	 * @param tableName
	 * @param startTime
	 * @param endTime
	 * @return
	 * @throws Exception
	 */
	public int queryCount(String tableName, RecorderQueryBuilder builder) throws Exception {
		Class<? extends IRecorder> tableClass = getTableClassByName(tableName);
		if (tableClass == null) {
			return 0;
		}
		return queryCount(tableClass, builder);
	}

	/**
	 * 查询日志条数
	 * 
	 * @param tableName
	 * @param startTime
	 * @param endTime
	 * @return
	 * @throws Exception
	 */
	public int queryCount(Class<? extends IRecorder> clss, RecorderQueryBuilder builder) throws Exception {
		if (isStop) {
			throw new Exception("server is stopped!");
		}
		String buildSelectTableSql = RecorderUtil.buildSelectCountTableSql_MYSQL(builder);
		try (Connection connection = dataSourceFactory.get().getConnection();
				PreparedStatement prepareStatement = connection.prepareStatement(buildSelectTableSql);
				ResultSet executeQuery = prepareStatement.executeQuery();) {
			executeQuery.next();
			int count = executeQuery.getInt(1);
			return count;
		}
	}

	public Collection<String> queryRelativeTables(Class<? extends IRecorder> clss, long startTime, long endTime)
			throws InstantiationException, IllegalAccessException, SQLException {
		// 获取相关表
		Set<String> relativeTableNames = RecorderUtil.getRelativeTableNames(clss, startTime, endTime);
		// 筛选不存在的表
		Iterator<String> iterator2 = relativeTableNames.iterator();
		try (Connection connection = dataSourceFactory.get().getConnection();) {
			List<String> tableNames = RecorderUtil.getTableNames(connection);
			for (; iterator2.hasNext();) {
				if (!tableNames.contains(iterator2.next())) {
					iterator2.remove();
				}
			}
		}
		return relativeTableNames;
	}

	/**
	 * 查询某段日期的日志
	 * 
	 * @param tableName
	 * @param startTime
	 * @param endTime
	 * @param startIndex
	 * @param size
	 * @param orderParam
	 * @return
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public <T extends IRecorder> List<T> query(String tableName, RecorderQueryBuilder builder) throws Exception {
		Class<? extends IRecorder> tableClass = getTableClassByName(tableName);
		if (tableClass == null) {
			return null;
		}
		return (List<T>) query(tableClass, builder);
	}

	/**
	 * 查询某段日期的日志
	 * 
	 * @param clss
	 * @param startTime
	 * @param endTime
	 * @return
	 * @throws Exception
	 */
	public <T extends IRecorder> List<T> query(Class<T> clss, RecorderQueryBuilder builder) throws Exception {
		if (isStop) {
			throw new Exception("server is stopped!");
		}
		List<T> result = new ArrayList<>();
		String buildSelectTableSql = RecorderUtil.buildSelectTableSql_MYSQL(builder);
		try (Connection connection = dataSourceFactory.get().getConnection();
				PreparedStatement prepareStatement = connection.prepareStatement(buildSelectTableSql);
				ResultSet executeQuery = prepareStatement.executeQuery();) {
			while (executeQuery.next()) {
				T newInstance = clss.newInstance();
				List<Field> logFields = RecorderUtil.getLogFields(clss);
				for (Field field : logFields) {
					Column annotation = field.getAnnotation(Column.class);
					if (annotation == null) {
						continue;
					}
					Object object = executeQuery.getObject(field.getName());
					if (object == null) {
						continue;
					}
					field.set(newInstance, object);
				}
				result.add(newInstance);
			}
		}
		return result;
	}
	
	/**
	 * 获取当前队列中日志任务的数量
	 * 
	 * @return
	 */
	public long getTaksCount() {
		return logTaskQueue.size();
	}

	public long getDoneLogNum() {
		return doneLogNum.longValue();
	}

	public long getLostLogNum() {
		return lostLogNum.longValue();
	}

	public RecorderProxy startServer() throws Exception {
		if (!isStop) {
			throw new NullPointerException("server has already start!");
		}
		// 初始化任务线程池
		if (customInsertThreadPool == null) {
			this.logTaskQueue = new LinkedBlockingQueue<>();
			this.threadPool = new ThreadPoolExecutor(threadCorePoolSize, threadMaximumPoolSize, 0,
					TimeUnit.MILLISECONDS, logTaskQueue, (runnable) -> {
						Thread t = new Thread(runnable, "LogDBServer-Insert-" + threadPool.getPoolSize());
						return t;
					});
		} else {
			this.logTaskQueue = customInsertThreadPool.getQueue();
			this.threadPool = customInsertThreadPool;
		}
		// 检查所有表的变更状况
		if (checker != null) {
			// 添加默认日志
			if (scanPackages != null && scanPackages.length > 0) {
				for (String packageName : scanPackages) {
					checker.registTable(packageName);
				}
			}
			// 启动时，执行表格结构检查
			try (Connection connection = dataSourceFactory.get().getConnection();) {
				checker.executeCheck(connection);
			}
		}
		this.isStop = false;
		return this;
	}

	public RecorderProxy stopServer() throws Exception {
		if (isStop) {
			throw new RecorderProxyAlreadyStopException();
		}
		this.isStop = true;
		List<Runnable> shutdownNow = threadPool.shutdownNow();
		// 完成剩余的任务
		for (Runnable task : shutdownNow) {
			try {
				task.run();
				log.info("save log tasks,remain:" + shutdownNow.size());
			} catch (Exception e) {
				log.error(e, e);
			}
		}
		shutdownNow.clear();
		log.info("db log system shutdone!");
		return this;
	}

	public static class RecorderProxyBuilder {
		// 扫描项目包名(日志结构检查)
		private Set<String> scanPackages = new HashSet<>();
		// 任务上限数量
		private int taskMaxSize;
		// 任务线程池基本线程数
		private int threadCorePoolSize;
		// 任务线程池最大线程数
		private int threadMaximumPoolSize;
		// 数据库引擎
		private String dbEngine;
		// 编码
		private String charset;
		// 自定义线程池
		private ThreadPoolExecutor customInsertThreadPool;
		private Supplier<DataSource> dataSourceFactory;

		public RecorderProxyBuilder() {
			this.taskMaxSize = 8000;
			// 任务线程池基本线程数
			this.threadCorePoolSize = 3;
			// 任务线程池最大线程数
			this.threadMaximumPoolSize = 5;
			// 数据库引擎
			this.dbEngine = "myisam";
			// 编码
			this.charset = "utf8";
		}

		/**
		 * 构建配置
		 * 
		 * @return
		 */
		public RecorderProxy build() {
			return new RecorderProxy(this);
		}

		/**
		 * 添加一个需要扫描的包
		 * 
		 * @param packageName
		 * @return
		 */
		public RecorderProxyBuilder addScanPackage(String... packageName) {
			for (String temp : packageName) {
				if (temp == null || temp.trim().length() == 0) {
					throw new NullPointerException("packageName");
				}
				this.scanPackages.add(temp);
			}
			return this;
		}

		/**
		 * 插入任务数量上限
		 * 
		 * @param size
		 * @return
		 */
		public RecorderProxyBuilder taskMaxSize(int size) {
			if (size > 0) {
				this.taskMaxSize = size;
			}
			return this;
		}

		/**
		 * 初始线程数大小
		 * 
		 * @param size
		 * @return
		 */
		public RecorderProxyBuilder threadCorePoolSize(int size) {
			if (size > 0) {
				this.threadCorePoolSize = size;
			}
			return this;
		}

		/**
		 * 最大线程数大小
		 * 
		 * @param size
		 * @return
		 */
		public RecorderProxyBuilder threadMaximumPoolSize(int size) {
			if (size > 0) {
				this.threadMaximumPoolSize = size;
			}
			return this;
		}

		/**
		 * 数据库引擎
		 * 
		 * @param dbEngine
		 * @return
		 */
		public RecorderProxyBuilder dbEngine(String dbEngine) {
			if (dbEngine == null || dbEngine.trim().length() == 0) {
				throw new NullPointerException("dbEngine");
			}
			this.dbEngine = dbEngine;
			return this;
		}

		/**
		 * 编码
		 * 
		 * @param charset
		 * @return
		 */
		public RecorderProxyBuilder charset(String charset) {
			if (charset == null) {
				throw new NullPointerException("charset");
			}
			this.charset = charset;
			return this;
		}

		/**
		 * 自定义一个线程池来处理(threadCorePoolSize和threadMaximumPoolSize将无效)
		 * 
		 * @param threadPool
		 * @return
		 */
		public RecorderProxyBuilder customInsertThreadPool(ThreadPoolExecutor threadPool) {
			if (threadPool == null) {
				throw new NullPointerException("customInsertThreadPool");
			}
			this.customInsertThreadPool = threadPool;
			return this;
		}

		public RecorderProxyBuilder dataSource(Supplier<DataSource> dataSourceFactory) {
			if (dataSourceFactory == null) {
				throw new NullPointerException("dataSourceFactory");
			}
			this.dataSourceFactory = dataSourceFactory;
			return this;
		}
	}
}