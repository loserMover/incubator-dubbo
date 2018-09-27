/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.dubbo.config.support;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Parameter
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface Parameter {
    /**
     * 描述：键（别名）
     * @return
     */
    String key() default "";
    /**
     * 描述：是否必填
     * @return
     */
    boolean required() default false;
    /**
     * 描述：是否忽略
     * @return
     */
    boolean excluded() default false;
    /**
     * 描述：是否转义
     * @return
     */
    boolean escaped() default false;
    /**
     * 描述：是否为属性
     * @return
     */
    boolean attribute() default false;
    /**
     * 描述：是否拼接默认属性
     * @return
     */
    boolean append() default false;

}