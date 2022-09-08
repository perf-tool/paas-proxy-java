/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.github.perftool.paas.proxy.controller;

import com.github.perftool.paas.proxy.module.SetValueReq;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.ConnectionPoolConfig;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/v1/redis")
public class RedisController {

    @RequestMapping("/keys/create")
    public ResponseEntity<String> create(@RequestBody SetValueReq setValueReq) {

        DefaultJedisClientConfig conf = DefaultJedisClientConfig.builder().password(setValueReq.getPassword()).build();
        Set<HostAndPort> set = Arrays.stream(setValueReq.getUrls().split(",")).map(url -> {
            String[] hostAndPort = url.split(":");
            return new HostAndPort(hostAndPort[0], Integer.parseInt(hostAndPort[1]));
        }).collect(Collectors.toSet());
        try (
                JedisCluster jedisClient = new JedisCluster(set, conf, setValueReq.getMaxAttempts(),
                        new ConnectionPoolConfig())
        ) {
            String ret = jedisClient.set(setValueReq.getKey(), setValueReq.getValue());
            return new ResponseEntity<>(ret, HttpStatus.OK);
        } catch (Exception e) {
            throw e;
        }
    }

    @RequestMapping("/keys/delete")
    public ResponseEntity<Void> delete(@RequestBody SetValueReq setValueReq) {

        DefaultJedisClientConfig conf = DefaultJedisClientConfig.builder().password(setValueReq.getPassword()).build();
        Set<HostAndPort> set = Arrays.stream(setValueReq.getUrls().split(",")).map(url -> {
            String[] hostAndPort = url.split(":");
            return new HostAndPort(hostAndPort[0], Integer.parseInt(hostAndPort[1]));
        }).collect(Collectors.toSet());
        try (
                JedisCluster jedisClient = new JedisCluster(set, conf, setValueReq.getMaxAttempts(),
                        new ConnectionPoolConfig())
        ) {
            long del = jedisClient.del(setValueReq.getKey());
            if (del == 0) {
                return new ResponseEntity<>(HttpStatus.OK);
            } else {
                return new ResponseEntity<>(HttpStatus.PRECONDITION_FAILED);
            }
        } catch (Exception e) {
            throw e;
        }
    }
}
