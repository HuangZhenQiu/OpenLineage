/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.flink.utils;

import static io.openlineage.flink.utils.Constants.BOOTSTRAP_SERVER;

import io.openlineage.flink.visitor.wrapper.WrapperUtils;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KafkaUtils {
  public static final String SECURITY_PROTOCOL = "security.protocol";
  public static final String SASL_SSL = "SASL_SSL";

  public static Optional<String> resolveBootstrapServerByKaffe(
      ClassLoader classLoader, Properties properties) {
    try {
      Class interceptorClass =
          classLoader.loadClass(
              "com.apple.pie.queue.kafka.client.configinterceptors.KaffeConfigurationInterceptor");
      Object kaffeConfigInterceptor = interceptorClass.getDeclaredConstructor().newInstance();
      log.debug("Using properties: {} to resolve brokerlist with Kaffe", properties);
      Map<String, Object> configMap = convert(properties);
      Class kaffeConfigClass =
          classLoader.loadClass("com.apple.pie.queue.kafka.client.kaffe.KaffeConfig");
      Object kaffeConfig =
          kaffeConfigClass.getDeclaredConstructor(Map.class).newInstance(configMap);
      Optional<Map<String, Object>> kaffeConfigMapOpt =
          WrapperUtils.<Map<String, Object>>invoke(
              kaffeConfig.getClass().getSuperclass(), kaffeConfig, "values");

      if (kaffeConfigMapOpt.isPresent()) {
        Map<String, Object> kaffeConfigMap = kaffeConfigMapOpt.get();
        Properties caffeProperties = new Properties();
        caffeProperties.putAll(kaffeConfigMap);
        log.debug("Using Kaffe properties: {} to resolve brokerlist with Kaffe", caffeProperties);
        invoke(kaffeConfigInterceptor.getClass(), kaffeConfigInterceptor, "configure", configMap);
        kaffeConfigMap.put(SECURITY_PROTOCOL, SASL_SSL);

        Optional<Map<String, Object>> configsOpts =
            invoke(
                kaffeConfigInterceptor.getClass(), kaffeConfigInterceptor, "apply", kaffeConfigMap);
        if (configsOpts.isPresent()) {
          return Optional.of(configsOpts.get().get(BOOTSTRAP_SERVER).toString());
        }
      }
    } catch (InstantiationException
        | IllegalAccessException
        | NoSuchMethodException
        | InvocationTargetException
        | ClassNotFoundException e) {
      log.debug("Can't use Kaffe to load configs", e);
      // do nothing here
    }
    return Optional.empty();
  }

  public static <T> Optional<T> invoke(
      Class aClass, Object object, String methodName, Map<String, Object> argument) {
    try {
      Method method = aClass.getDeclaredMethod(methodName, Map.class);
      method.setAccessible(true);
      return Optional.ofNullable((T) method.invoke(object, argument));
    } catch (NoSuchMethodException e) {
      log.error("Method {} not found in class {}", methodName, aClass, e);
      return Optional.empty();
    } catch (IllegalAccessException | InvocationTargetException e) {
      log.error("Method {} invocation failed in class {}", methodName, aClass, e);
      return Optional.empty();
    }
  }

  public static String convertToNamespace(Optional<String> servers) {
    String server =
        servers
            .map(
                str -> {
                  if (!str.matches("\\w+://.*")) {
                    return "PLAINTEXT://" + str;
                  } else {
                    return str;
                  }
                })
            .map(str -> URI.create(str.split(",")[0]))
            .map(uri -> uri.getHost() + ":" + uri.getPort())
            .orElse("");
    String namespace = "kafka://" + server;
    return namespace;
  }

  private static Map<String, Object> convert(Properties properties) {
    Map<String, Object> map = new HashMap<>();
    for (final String name : properties.stringPropertyNames()) {
      map.put(name, properties.getProperty(name));
    }
    return map;
  }
}
