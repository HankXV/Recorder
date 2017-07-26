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
	 * @return
	 */
	String value() default "";

	/**
	 * 在sql里的类型
	 * 
	 * @return
	 */
	SQLType type() default SQLType.INT;

	int size() default 0;

	boolean isNullable() default true;

	/**
	 * 是否是主键
	 * 
	 * @return
	 */
	boolean primary() default false;
}