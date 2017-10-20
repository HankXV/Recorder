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

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * 日志查询构造器
 *
 * @author hank
 */
public class RecorderQueryBuilder {
    private StringBuilder selections = new StringBuilder();
    private StringBuilder tableNames = new StringBuilder();
    private String where;
    private StringBuilder orderBySb = new StringBuilder();
    private StringBuilder groupBySb = new StringBuilder();
    private String limit;
    private List<RecorderQueryBuilder> unions = new ArrayList<>();

    public RecorderQueryBuilder unionAll(RecorderQueryBuilder builder) throws Exception {
        if (builder.hashCode() == hashCode()) {
            throw new RecorderQueryBuilderException("can not add self!");
        }
        unions.add(builder);
        return this;
    }

    public RecorderQueryBuilder select(String value) {
        selections.append(Objects.requireNonNull(value)).append(",");
        return this;
    }

    public RecorderQueryBuilder tables(Collection<String> tables) {
        for (String tb : Objects.requireNonNull(tables)) {
            tableNames.append(tb).append(",");
        }
        return this;
    }

    public RecorderQueryBuilder tables(RecorderQueryBuilder table) throws Exception {
        if (table.hashCode() == hashCode()) {
            throw new RecorderQueryBuilderException("can not add self!");
        }
        tableNames.append("(").append(table.build()).append(") as atlas_").append(Integer.toHexString(table.hashCode())).append(",");
        return this;
    }

    public RecorderQueryBuilder tables(String... tables) {
        for (String table : Objects.requireNonNull(tables)) {
            tableNames.append(Objects.requireNonNull(table)).append(",");
        }
        return this;
    }

    public RecorderQueryBuilder where(WhereConditionBuilder condition) throws Exception {
        where = condition.build();
        return this;
    }

    public RecorderQueryBuilder limit(int start, int size) {
        limit = "limit " + start + "," + size;
        return this;
    }

    public RecorderQueryBuilder orderBy(String fieldName, boolean desc) {
        orderBySb.append(Objects.requireNonNull(fieldName)).append(desc ? " desc" : " asc").append(",");
        return this;
    }

    public RecorderQueryBuilder groupBy(String fieldName) {
        groupBySb.append(Objects.requireNonNull(fieldName)).append(",");
        return this;
    }

    public String build() throws RecorderQueryBuilderException {
        String source = "select {0} from {1} {2} {3} {4} {5}";
        if (selections.length() == 0) {
            throw new RecorderQueryBuilderException("no selection item!");
        }
        StringBuilder selectionsCopy = new StringBuilder(selections.toString());
        selectionsCopy.deleteCharAt(selectionsCopy.length() - 1);
        if (tableNames.length() == 0) {
            throw new RecorderQueryBuilderException("no table item!");
        }
        StringBuilder tableNamesCopy = new StringBuilder(tableNames.toString());
        tableNamesCopy.deleteCharAt(tableNamesCopy.length() - 1);
        StringBuilder groupBySbCopy = new StringBuilder(groupBySb.toString());
        if (groupBySb.length() != 0) {
            groupBySbCopy.deleteCharAt(groupBySbCopy.length() - 1);
        }
        StringBuilder orderBySbCopy = new StringBuilder(orderBySb.toString());
        if (orderBySb.length() != 0) {
            orderBySbCopy.deleteCharAt(orderBySbCopy.length() - 1);
        }
        String format = MessageFormat.format(source, selectionsCopy.toString(), tableNamesCopy.toString(),
                where == null ? "" : "where " + where,
                groupBySbCopy.length() == 0 ? "" : "group by" + groupBySbCopy.toString(),
                orderBySbCopy.length() == 0 ? "" : "order by " + orderBySbCopy.toString(), limit == null ? "" : limit);
        if (!unions.isEmpty()) {
            for (RecorderQueryBuilder temp : unions) {
                format += " union all " + temp.build();
            }
        }
        return format;
    }

    public static final class WhereConditionBuilder {
        private StringBuilder sb = new StringBuilder();
        private int qouteSignal = 0;
        private int contactSignal = 0;

        public WhereConditionBuilder qouteStart() {
            sb.append("(");
            ++qouteSignal;
            return this;
        }

        public WhereConditionBuilder qouteEnd() {
            sb.append(")");
            --qouteSignal;
            return this;
        }

        public WhereConditionBuilder and() throws Exception {
            if (contactSignal != 0) {
                throw new RecorderQueryBuilderException("there is more contact exists!");
            }
            sb.append(" and ");
            ++contactSignal;
            return this;
        }

        public WhereConditionBuilder or() throws Exception {
            if (contactSignal != 0) {
                throw new RecorderQueryBuilderException("there is more contact exists!");
            }
            sb.append(" or ");
            ++contactSignal;
            return this;
        }

        public WhereConditionBuilder lt(String fieldName, Object value, boolean isClosure) {
            sb.append(Objects.requireNonNull(fieldName)).append(" <").append(isClosure ? "= " : " ")
                    .append(Objects.requireNonNull(value).toString());
            if (contactSignal > 0) {
                --contactSignal;
            }
            return this;
        }

        public WhereConditionBuilder gt(String fieldName, Object value, boolean isClosure) {
            sb.append(Objects.requireNonNull(fieldName)).append(" >").append(isClosure ? "= " : " ")
                    .append(Objects.requireNonNull(value).toString());
            if (contactSignal > 0) {
                --contactSignal;
            }
            return this;
        }

        public WhereConditionBuilder eq(String fieldName, Object value) {
            sb.append(Objects.requireNonNull(fieldName)).append(" = ").append(Objects.requireNonNull(value).toString());
            if (contactSignal > 0) {
                --contactSignal;
            }
            return this;
        }

        public WhereConditionBuilder notEq(String fieldName, Object value) {
            sb.append(Objects.requireNonNull(fieldName)).append(" != ")
                    .append(Objects.requireNonNull(value).toString());
            if (contactSignal > 0) {
                --contactSignal;
            }
            return this;
        }

        public WhereConditionBuilder like(String fieldName, Object value, boolean left, boolean right) {
            sb.append(Objects.requireNonNull(fieldName)).append(" like '").append(left ? "%" : "")
                    .append(value == null ? "" : value.toString()).append(right ? "%" : "").append("'");
            if (contactSignal > 0) {
                --contactSignal;
            }
            return this;
        }

        private String build() throws Exception {
            if (qouteSignal != 0) {
                throw new RecorderQueryBuilderException("qoute count error," + qouteSignal);
            }
            if (contactSignal != 0) {
                throw new RecorderQueryBuilderException("contant count error," + qouteSignal);
            }
            if (sb.length() == 0) {
                return "";
            }
            return sb.toString();
        }
    }
}
