/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.api.table.factory;

import org.apache.seatunnel.api.configuration.util.OptionRule;

/** todo: use PluginIdentifier. This is the SPI interface. */
public interface Factory {

    /**
     * Returns a unique identifier among same factory interfaces.
     *
     * 进行连接器标识，每个连接器应该唯一
     *
     * <p>For consistency, an identifier should be declared as one lower case word (e.g. {@code
     * kafka}). If multiple factories exist for different versions, a version should be appended
     * using "-" (e.g. {@code elasticsearch-7}).
     */
    String factoryIdentifier();

    /**
     * Returns the rule for options.
     *
     * 声明该连接器所需要的参数，哪些是必填，哪些是选填，哪些一起填会存在冲突等等，在创建连接器时会先对配置参数来进行校验.
     *
     * <p>1. Used to verify whether the parameters configured by the user conform to the rules of
     * the options;
     *
     * <p>2. Used for Web-UI to prompt user to configure option value;
     */
    OptionRule optionRule();
}
