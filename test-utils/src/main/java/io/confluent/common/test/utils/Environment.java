/*-
 * Copyright (C) 2022-2024 Confluent, Inc.
 */

package io.confluent.common.test.utils;

import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.StringUtils;

@UtilityClass
public class Environment {

  public boolean isARM() {
    return StringUtils.containsIgnoreCase(System.getProperty("os.arch"), "arm");
  }
}
