/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.wrapper;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.FieldUtils;

/**
 * Class with utility methods related to make use of {@link
 * org.apache.commons.lang3.reflect.FieldUtils} and {@link
 * org.apache.commons.lang3.reflect.MethodUtils}
 */
@SuppressWarnings("PMD")
@Slf4j
public class WrapperUtils {

  /**
   * Gets a field of a given object
   *
   * @param aClass class to find a field
   * @param object object to extract field value
   * @param field fielnd name
   * @param <T> returned type
   * @return field valuesrc/main/java/io/openlineage/flink/visitor/wrapper/WrapperUtils.java
   */
  public static <T> Optional<T> getFieldValue(Class aClass, Object object, String field) {
    try {
      return Optional.ofNullable((T) FieldUtils.getField(aClass, field, true).get(object));
    } catch (IllegalAccessException
        | ClassCastException
        | NullPointerException
        | IllegalArgumentException e) {
      log.error("cannot extract field {} from {}", field, aClass.getName(), e);
      return Optional.empty();
    }
  }

  /**
   * Set a field of a given object
   *
   * @param aClass class to find a field
   * @param aObject object that field value will beset
   * @param fieldValue object as the field value
   * @param field field name
   */
  public static void setFieldValue(Class aClass, Object aObject, Object fieldValue, String field) {
    try {
      FieldUtils.getField(aClass, field, true).set(aObject, fieldValue);
    } catch (IllegalAccessException
        | ClassCastException
        | NullPointerException
        | IllegalArgumentException e) {
      log.error("cannot extract field {} from {}", field, aClass.getName(), e);
    }
  }

  public static <T> Optional<T> invoke(Class aClass, Object object, String methodName) {
    try {
      Method method = aClass.getDeclaredMethod(methodName);
      method.setAccessible(true);
      return Optional.ofNullable((T) method.invoke(object));
    } catch (NoSuchMethodException e) {
      log.error("Method {} not found in class {}", methodName, aClass, e);
      return Optional.empty();
    } catch (IllegalAccessException | InvocationTargetException e) {
      log.error("Method {} invocation failed in class {}", methodName, aClass, e);
      return Optional.empty();
    }
  }

  public static <T> Optional<T> invokeStatic(Class aClass, String method) {
    try {
      return Optional.ofNullable((T) aClass.getMethod(method).invoke(null));
    } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
      log.error("cannot call schema on produced class", e);
      return Optional.empty();
    }
  }
}
