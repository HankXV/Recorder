package org.slingerxv.recorder;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;



/**
 * 日志列注解
 * 
 * @author hank
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Column {

	/**
	 * sql类型
	 * 
	 * @return sql类型
	 */
    SQLType type() default SQLType.MYSQL_int;

	/**
	 * 大小
	 * 
	 * @return 大小
	 */
    int size() default 0;

	/**
	 * 列注释
	 * 
	 * @return 列注释
	 */
    String comment() default "N/A";
}
