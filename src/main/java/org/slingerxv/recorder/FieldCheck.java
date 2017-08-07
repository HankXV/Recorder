package org.slingerxv.recorder;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 字段对应表信息检查
 * 
 * @author hank
 *
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface FieldCheck {
	/**
	 * 表字段名
	 * 
	 * @return 字段名称
	 */
	String value() default "";

	/**
	 * 在sql里的类型
	 * 
	 * @return sql类型
	 */
	SQLType type() default SQLType.INT;

	int size() default 0;

	/**
	 * 是否可以为空
	 * 
	 * @return 是否为可以为空
	 */
	boolean isNullable() default true;

	/**
	 * 是否是主键
	 * 
	 * @return 是否是主键
	 */
	boolean primary() default false;
}