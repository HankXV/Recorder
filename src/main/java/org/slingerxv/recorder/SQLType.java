package org.slingerxv.recorder;

/**
 * Sql数据类型
 * 
 * @author hank
 *
 */
public enum SQLType {
	//
	MYSQL_tinyint("tinyint"),
	//
	MYSQL_smallint("smallint"),
	//
	MYSQL_mediumint("mediumint"),
	//
	MYSQL_int("int"),
	//
	MYSQL_integer("integer"),
	//
	MYSQL_bigint("bigint"),
	//
	MYSQL_bit("bit"),
	//
	MYSQL_real("real"),
	//
	MYSQL_double("double"),
	//
	MYSQL_float("float"),
	//
	MYSQL_decimal("decimal"),
	//
	MYSQL_numeric("numeric"),
	//
	MYSQL_char("char"),
	//
	MYSQL_varchar("varchar"),
	//
	MYSQL_date("date"),
	//
	MYSQL_time("time"),
	//
	MYSQL_year("year"),
	//
	MYSQL_timestamp("timestamp"),
	//
	MYSQL_datetime("datetime"),
	//
	MYSQL_tinyblob("tinyblob"),
	//
	MYSQL_blob("blob"),
	//
	MYSQL_mediumblob("mediumblob"),
	//
	MYSQL_longblob("longblob"),
	//
	MYSQL_tinytext("tinytext"),
	//
	MYSQL_text("text"),
	//
	MYSQL_mediumtext("mediumtext"),
	//
	MYSQL_longtext("longtext"),
	//
	MYSQL_enum("enum"),
	//
	MYSQL_set("set"),
	//
	MYSQL_binary("binary"),
	//
	MYSQL_varbinary("varbinary"),;
	private String value;

	SQLType(String value) {
		this.value = value;
	}

	public String getValue() {
		return value;
	}

	public static SQLType getTypeByValue(String value) {
		for (SQLType temp : SQLType.values()) {
			if (temp.getValue().equals(value)) {
				return temp;
			}
		}
		return null;
	}
}
