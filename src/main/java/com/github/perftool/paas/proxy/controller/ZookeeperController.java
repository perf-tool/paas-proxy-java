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

import com.github.perftool.paas.proxy.module.CreateNodeResp;
import com.github.perftool.paas.proxy.module.CreateNodeReq;
import com.github.perftool.paas.proxy.module.DeleteNodeReq;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.nio.charset.StandardCharsets;

@RestController
@RequestMapping("/v1/zookeeper")
public class ZookeeperController {

    @PostMapping("/nodes/create")
    public ResponseEntity<CreateNodeResp> createLedger(@RequestBody CreateNodeReq req) throws Exception {
        try (
                ZooKeeper zk = new ZooKeeper(String.format("%s:%d", req.getHost(), req.getPort()), 3000,
                        watchedEvent -> System.out.println("zk process : " + watchedEvent))
        ) {
            String path = zk.create(req.getPath(), req.getData().getBytes(StandardCharsets.UTF_8)
                    , ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            CreateNodeResp resp = new CreateNodeResp();
            resp.setPath(path);
            return new ResponseEntity<>(resp, HttpStatus.OK);
        } catch (Exception e) {
            throw e;
        }
    }

    @PostMapping("/nodes/delete")
    public ResponseEntity<Void> deleteLedger(@RequestBody DeleteNodeReq req) throws Exception {
        try (
                ZooKeeper zk = new ZooKeeper(String.format("%s:%d", req.getHost(), req.getPort()), 3000,
                        watchedEvent -> System.out.println("zk process : " + watchedEvent));
        ) {
            zk.delete(req.getPath(), req.getVersion());
            return new ResponseEntity<>(HttpStatus.OK);
        } catch (Exception e) {
            throw e;
        }
    }
}
