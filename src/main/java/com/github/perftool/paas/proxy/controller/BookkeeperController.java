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

import com.github.perftool.paas.proxy.module.CreateLedgerReq;
import com.github.perftool.paas.proxy.module.CreateLedgerResp;
import com.github.perftool.paas.proxy.module.DeleteLedgerReq;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.nio.charset.StandardCharsets;

@RestController
@RequestMapping("/v1/bookkeeper")
public class BookkeeperController {

    private static final byte[] PASSWORD = "".getBytes(StandardCharsets.UTF_8);

    @GetMapping("/hello")
    public ResponseEntity<String> hello() {
        return new ResponseEntity<>("Hello, Bookkeeper", HttpStatus.OK);
    }

    @PostMapping("/ledgers/create")
    public ResponseEntity<CreateLedgerResp> createLedger(@RequestBody CreateLedgerReq req) throws Exception {
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setMetadataServiceUri("zk+null://" + req.getZkServers() + "/ledgers");
        try (BookKeeper bkc = new BookKeeper(clientConfiguration)) {
            try (LedgerHandle lh = bkc.createLedger(1, 1, 1, BookKeeper.DigestType.CRC32, PASSWORD)) {
                CreateLedgerResp resp = new CreateLedgerResp();
                resp.setLedgerId(lh.getId());
                return new ResponseEntity<>(resp, HttpStatus.OK);
            }
        }
    }

    @PostMapping("/ledgers/delete")
    public ResponseEntity<Void> deleteLedger(@RequestBody DeleteLedgerReq req) throws Exception {
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setMetadataServiceUri("zk+null://" + req.getZkServers() + "/ledgers");
        try (BookKeeper bkc = new BookKeeper(clientConfiguration)) {
            bkc.deleteLedger(req.getLedgerId());
        }
        return new ResponseEntity<>(HttpStatus.OK);
    }

}
