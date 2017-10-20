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

import java.io.IOException;
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
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 数据库日志记录服务器
 *
 * @author hank
 */
public class RecorderProxy {
    private static Logger log = LoggerFactory.getLogger(RecorderProxy.class);
    private ThreadPoolExecutor threadPool;
    private BlockingQueue<Runnable> logTaskQueue;
    private final RecorderChecker checker = new RecorderChecker();
    private boolean isStop = true;
    private final LongAdder doneLogNum = new LongAdder();
    private final LongAdder lostLogNum = new LongAdder();
    // -- config
    // 扫描项目包名(日志结构检查)
    private String[] scanPackages;
    // 任务线程池基本线程数
    private final int threadCorePoolSize;
    // 任务线程池最大线程数
    private final int threadMaximumPoolSize;
    // 任务上限数量
    private final int taskMaxSize;
    // 数据库引擎
    private final String dbEngine;
    // 编码
    private final String charset;
    // 自定义线程池
    private final ThreadPoolExecutor customInsertThreadPool;
    private final Supplier<DataSource> dataSourceFactory;

    private RecorderProxy(RecorderProxyBuilder builder) {
        this.scanPackages = builder.scanPackages.toArray(new String[0]);
        this.taskMaxSize = builder.taskMaxSize;
        this.threadCorePoolSize = builder.threadCorePoolSize;
        this.threadMaximumPoolSize = builder.threadMaximumPoolSize;
        this.dbEngine = builder.dbEngine;
        this.charset = builder.charset;
        this.customInsertThreadPool = builder.customInsertThreadPool;
        this.dataSourceFactory = Objects.requireNonNull(builder.dataSourceFactory, "dataSourceFactory");
    }

    /**
     * 执行一条日志记录的插入
     *
     * @param baseLog 日志实体
     * @return
     * @throws RecorderProxyAlreadyStopException 代理已经停止异常
     * @throws RecorderTaskOverloadException     代理任务队列超过限制异常
     */
    public RecorderProxy execute(final IRecorder alog)
            throws RecorderProxyStateException, RecorderTaskOverloadException {
        if (isStop) {
            throw new RecorderProxyStateException("stop");
        }

        if (alog != null) {
            if (getTaksCount() > taskMaxSize) {
                lostLogNum.increment();
                throw new RecorderTaskOverloadException("task count is overload,drop task:" + alog);
            }
            threadPool.execute(() -> {
                try (Connection con = dataSourceFactory.get().getConnection()) {
                    long now = System.currentTimeMillis();
                    String buildExistTableSql_MYSQL = RecorderUtil
                            .buildExistTableSqlMYSQL(RecorderUtil.getLogTableName(alog, now));
                    try (PreparedStatement existStatement = con.prepareStatement(buildExistTableSql_MYSQL);
                         ResultSet executeQuery = existStatement.executeQuery()) {
                        if (!executeQuery.next()) {
                            String buildCreateTableSql = RecorderUtil.buildCreateTableSqlMYSQL(alog, dbEngine, charset);
                            try (PreparedStatement createStatement = con.prepareStatement(buildCreateTableSql)) {
                                // 执行创建表
                                createStatement.executeUpdate();
                            }

                        }
                    }
                    String buildInsertTableSql = RecorderUtil.buildInsertTableSqlMYSQL(alog);
                    try (PreparedStatement insertStatement = con.prepareStatement(buildInsertTableSql)) {
                        // 执行插入
                        if (insertStatement.executeUpdate() > 0) {
                            doneLogNum.increment();
                        } else {
                            log.error("log failed:" + alog);
                            lostLogNum.increment();
                        }
                    }
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                    log.error("log failed:" + alog);
                    lostLogNum.increment();
                }
            });
        }
        return this;
    }

    /**
     * 通过表名获取其类
     *
     * @param tableName 表名
     * @return 表名对应的类
     */
    public Class<? extends IRecorder> getTableClassByName(String tableName) {
        return checker.getTableClass(tableName);
    }

    /**
     * 查询日志条数
     *
     * @param tableName 表名
     * @param startTime 开始时间戳
     * @param endTime   结束时间戳
     * @return 日志条数
     * @throws SQLException
     * @throws RecorderQueryBuilderException
     * @throws RecorderProxyAlreadyStopException
     */
    public int queryCount(String tableName, RecorderQueryBuilder builder)
            throws RecorderProxyStateException, RecorderQueryBuilderException, SQLException {
        Class<? extends IRecorder> tableClass = getTableClassByName(tableName);
        if (tableClass == null) {
            return 0;
        }
        return queryCount(tableClass, builder);
    }

    /**
     * 查询日志条数
     *
     * @param tableName 表名
     * @param startTime 开始时间戳
     * @param endTime   结束时间戳
     * @return
     * @throws RecorderProxyAlreadyStopException
     * @throws RecorderQueryBuilderException
     * @throws SQLException
     */
    public int queryCount(Class<? extends IRecorder> clss, RecorderQueryBuilder builder)
            throws RecorderProxyStateException, RecorderQueryBuilderException, SQLException {
        if (isStop) {
            throw new RecorderProxyStateException("stop");
        }
        String buildSelectTableSql = RecorderUtil.buildSelectCountTableSqlMYSQL(builder);
        try (Connection connection = dataSourceFactory.get().getConnection();
             PreparedStatement prepareStatement = connection.prepareStatement(buildSelectTableSql);
             ResultSet executeQuery = prepareStatement.executeQuery()) {
            executeQuery.next();
            return executeQuery.getInt(1);
        }
    }

    /**
     * 查找相关的表
     *
     * @param clss      表类
     * @param startTime 开始时间戳
     * @param endTime   结束时间戳
     * @return
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws SQLException
     */
    public Collection<String> queryRelativeTables(Class<? extends IRecorder> clss, long startTime, long endTime)
            throws InstantiationException, IllegalAccessException, SQLException {
        // 获取相关表
        Set<String> relativeTableNames = RecorderUtil.getRelativeTableNames(clss, startTime, endTime);
        // 筛选不存在的表
        Iterator<String> iterator2 = relativeTableNames.iterator();
        try (Connection connection = dataSourceFactory.get().getConnection()) {
            List<String> tableNames = RecorderUtil.getTableNames(connection);
            for (; iterator2.hasNext(); ) {
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
     * @param tableName  表名
     * @param startTime  开始时间戳
     * @param endTime    结束时间戳
     * @param startIndex 分页索引
     * @param size       每页大小
     * @param orderParam 排序参数
     * @return
     * @throws SQLException
     * @throws RecorderQueryBuilderException
     * @throws RecorderProxyAlreadyStopException
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    public <T extends IRecorder> List<T> query(String tableName, RecorderQueryBuilder builder)
            throws InstantiationException, IllegalAccessException, RecorderProxyStateException,
            RecorderQueryBuilderException, SQLException {
        Class<? extends IRecorder> tableClass = getTableClassByName(tableName);
        if (tableClass == null) {
            return null;
        }
        return (List<T>) query(tableClass, builder);
    }

    /**
     * 查询某段日期的日志
     *
     * @param clss      日志类
     * @param startTime 开始时间戳
     * @param endTime   结束时间戳
     * @return
     * @throws RecorderProxyAlreadyStopException
     * @throws RecorderQueryBuilderException
     * @throws SQLException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public <T extends IRecorder> List<T> query(Class<T> clss, RecorderQueryBuilder builder)
            throws RecorderProxyStateException, RecorderQueryBuilderException, SQLException, InstantiationException,
            IllegalAccessException {
        if (isStop) {
            throw new RecorderProxyStateException("stop");
        }
        List<T> result = new ArrayList<>();
        String buildSelectTableSql = RecorderUtil.buildSelectTableSqlMYSQL(builder);
        try (Connection connection = dataSourceFactory.get().getConnection();
             PreparedStatement prepareStatement = connection.prepareStatement(buildSelectTableSql);
             ResultSet executeQuery = prepareStatement.executeQuery()) {
            while (executeQuery.next()) {
                T newInstance = clss.newInstance();
                List<Field> logFields = RecorderUtil.getLogFields(clss);
                for (Field field : logFields) {
                    Col annotation = field.getAnnotation(Col.class);
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

    public long getTaksCount() {
        return logTaskQueue.size();
    }

    public long getDoneLogNum() {
        return doneLogNum.longValue();
    }

    public long getLostLogNum() {
        return lostLogNum.longValue();
    }

    /**
     * 开启代理
     *
     * @return this
     * @throws RecorderProxyAlreadyStartException
     * @throws RecorderCheckException
     * @throws SQLException
     * @throws ClassNotFoundException
     * @throws IOException
     */
    public RecorderProxy startServer() throws RecorderProxyStateException, RecorderCheckException, SQLException,
            ClassNotFoundException, IOException {
        if (!isStop) {
            throw new RecorderProxyStateException("stop");
        }
        // 初始化任务线程池
        if (customInsertThreadPool == null) {
            this.logTaskQueue = new LinkedBlockingQueue<>();
            this.threadPool = new ThreadPoolExecutor(threadCorePoolSize, threadMaximumPoolSize, 0,
                    TimeUnit.MILLISECONDS, logTaskQueue, (Runnable runnable) -> {
                return new Thread(runnable, "RecorderProxy-Insert-" + threadPool.getPoolSize());
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
            try (Connection connection = dataSourceFactory.get().getConnection()) {
                checker.executeCheck(connection);
            }
        }
        this.isStop = false;
        return this;
    }

    /**
     * 停止代理
     *
     * @return this
     * @throws RecorderProxyAlreadyStopException
     */
    public RecorderProxy stopServer() throws RecorderProxyStateException {
        if (isStop) {
            throw new RecorderProxyStateException("stop");
        }
        this.isStop = true;
        List<Runnable> shutdownNow = threadPool.shutdownNow();
        // 完成剩余的任务
        for (Runnable task : shutdownNow) {
            try {
                task.run();
                log.info("save log tasks,remain:" + shutdownNow.size());
            } catch (Exception e) {
                log.error(e.getMessage(), e);
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
        public RecorderProxyBuilder addScanPackage(final String... packageNames) {
            for (final String temp : Objects.requireNonNull(packageNames, "packageNames")) {
                this.scanPackages.add(Objects.requireNonNull(temp, "packageName"));
            }
            return this;
        }

        /**
         * 插入任务数量上限
         *
         * @param size
         * @return
         */
        public RecorderProxyBuilder taskMaxSize(final int size) {
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
        public RecorderProxyBuilder threadCorePoolSize(final int size) {
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
        public RecorderProxyBuilder threadMaximumPoolSize(final int size) {
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
        public RecorderProxyBuilder dbEngine(final String dbEngine) {
            this.dbEngine = Objects.requireNonNull(dbEngine, "dbEngine");
            return this;
        }

        /**
         * 编码
         *
         * @param charset
         * @return
         */
        public RecorderProxyBuilder charset(final String charset) {
            this.charset = Objects.requireNonNull(charset, "charset");
            return this;
        }

        /**
         * 自定义一个线程池来处理(threadCorePoolSize和threadMaximumPoolSize将无效)
         *
         * @param threadPool
         * @return
         */
        public RecorderProxyBuilder customInsertThreadPool(final ThreadPoolExecutor threadPool) {
            this.customInsertThreadPool = Objects.requireNonNull(threadPool, "threadPool");
            return this;
        }

        public RecorderProxyBuilder dataSource(final Supplier<DataSource> dataSourceFactory) {
            this.dataSourceFactory = Objects.requireNonNull(dataSourceFactory, "dataSourceFactory");
            return this;
        }
    }
}