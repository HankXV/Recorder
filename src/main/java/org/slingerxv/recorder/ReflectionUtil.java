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

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.function.Function;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * 反射工具
 */
public final class ReflectionUtil {
    private ReflectionUtil() {
    }

    public static List<Field> getFields(Class<?> clazz, boolean isSuper) {
        return getFields(clazz, isSuper, field -> !Modifier.isStatic(field.getModifiers()));
    }

    /**
     * 获取类所有字段
     *
     * @param clazz
     * @param isSuper 是否迭代检查超类
     * @return
     */
    public static List<Field> getFields(Class<?> clazz, boolean isSuper, Function<Field, Boolean> filter) {
        List<Field> list = new ArrayList<>();
        Class<?> nextClass = clazz;
        do {
            Field[] declaredFields = nextClass.getDeclaredFields();
            for (Field field : declaredFields) {
                if (!filter.apply(field)) {
                    continue;
                }
                list.add(field);
            }
            nextClass = nextClass.getSuperclass();
        } while (nextClass != Object.class && isSuper);
        return list;
    }

    /**
     * 扫描包里符合条件的类
     *
     * @param packageName
     * @param superClass
     * @return
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public static List<Class<?>> getClassesByPackage(String packageName, Class<?> superClass)
            throws IOException, ClassNotFoundException {
        // 第一个class类的集合
        List<Class<?>> classes = new ArrayList<>();
        // 获取包的名字 并进行替换
        String packageDirName = packageName.replace('.', '/');
        // 定义一个枚举的集合 并进行循环来处理这个目录下的things
        Enumeration<URL> dirs = Thread.currentThread().getContextClassLoader().getResources(packageDirName);
        // 循环迭代下去
        while (dirs.hasMoreElements()) {
            // 获取下一个元素
            URL url = dirs.nextElement();
            // 得到协议的名称
            String protocol = url.getProtocol();
            // 如果是以文件的形式保存在服务器上
            if ("file".equals(protocol)) {
                // 获取包的物理路径
                String filePath = URLDecoder.decode(url.getFile(), "UTF-8");
                // 以文件的方式扫描整个包下的文件 并添加到集合中
                findAndAddClassesInPackageByFile(packageName, filePath, true, classes, superClass);
            } else if ("jar".equals(protocol)) {
                // 如果是jar包文件
                // 定义一个JarFile
                JarFile jar = ((JarURLConnection) url.openConnection()).getJarFile();
                // 从此jar包 得到一个枚举类
                Enumeration<JarEntry> entries = jar.entries();
                // 同样的进行循环迭代
                while (entries.hasMoreElements()) {
                    // 获取jar里的一个实体 可以是目录 和一些jar包里的其他文件 如META-INF等文件
                    JarEntry entry = entries.nextElement();
                    String name = entry.getName();
                    // 如果是以/开头的
                    if (name.charAt(0) == '/') {
                        // 获取后面的字符串
                        name = name.substring(1);
                    }
                    // 如果前半部分和定义的包名相同
                    if (!name.startsWith(packageDirName)) {
                        continue;
                    }
                    int idx = name.lastIndexOf('/');
                    // 如果以"/"结尾 是一个包
                    if (idx != -1) {
                        // 获取包名 把"/"替换成"."
                        packageName = name.substring(0, idx).replace('/', '.');
                    }
                    // 如果是一个.class文件 而且不是目录
                    if (!name.endsWith(".class") || entry.isDirectory()) {
                        continue;
                    }
                    // 去掉后面的".class" 获取真正的类名
                    String className = name.substring(packageName.length() + 1, name.length() - 6);
                    Class<?> loadClass = Thread.currentThread().getContextClassLoader()
                            .loadClass(packageName + '.' + className);
                    if ((superClass.isAssignableFrom(loadClass)) && (!(superClass.equals(loadClass)))) {
                        classes.add(loadClass);
                    }
                }
            }
        }
        return classes;
    }

    /**
     * 以文件的形式来获取包下的所有Class
     *
     * @param packageName
     * @param packagePath
     * @param recursive
     * @param classes
     * @throws ClassNotFoundException
     */
    private static void findAndAddClassesInPackageByFile(String packageName, String packagePath,
                                                         final boolean recursive, List<Class<?>> classes, Class<?> superClass) {
        // 获取此包的目录 建立一个File
        File dir = new File(packagePath);
        // 如果不存在或者 也不是目录就直接返回
        if (!dir.exists() || !dir.isDirectory()) {
            return;
        }
        // 如果存在 就获取包下的所有文件 包括目录
        // 自定义过滤规则 如果可以循环(包含子目录) 或则是以.class结尾的文件(编译好的java类文件)
        File[] dirfiles = dir
                .listFiles(file -> (recursive && file.isDirectory()) || (file.getName().endsWith(".class")));
        if (dirfiles == null) {
            return;
        }
        // 循环所有文件
        for (File file : dirfiles) {
            // 如果是目录 则继续扫描
            if (file.isDirectory()) {
                findAndAddClassesInPackageByFile(packageName + "." + file.getName(), file.getAbsolutePath(), recursive,
                        classes, superClass);
            } else {
                // 如果是java类文件 去掉后面的.class 只留下类名
                String className = file.getName().substring(0, file.getName().length() - 6);
                Class<?> loadClass;
                try {
                    loadClass = Thread.currentThread().getContextClassLoader().loadClass(packageName + '.' + className);
                } catch (ClassNotFoundException e) {
                    continue;
                }
                if ((superClass.isAssignableFrom(loadClass)) && (!(superClass.equals(loadClass)))) {
                    classes.add(loadClass);
                }
            }
        }
    }
}
