/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.server.security.handler;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.accumulo.server.ServerContext;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZKAuthorizor implements Authorizor {
  private static final Logger log = LoggerFactory.getLogger(ZKAuthorizor.class);

  private final String ZKUserAuths = "/Authorizations";

  private ServerContext context;
  private String zkUserPath;

  @Override
  public void initialize(ServerContext context) {
    this.context = context;
    zkUserPath = context.zkUserPath();
  }

  @Override
  public Authorizations getCachedUserAuthorizations(String user) {
    byte[] authsBytes = context.getZooCache().get(zkUserPath + "/" + user + ZKUserAuths);
    if (authsBytes != null) {
      return ZKSecurityTool.convertAuthorizations(authsBytes);
    }
    return Authorizations.EMPTY;
  }

  @Override
  public boolean validSecurityHandlers(Authenticator auth, PermissionHandler pm) {
    return true;
  }

  @Override
  public void initializeSecurity(TCredentials itw, String rootuser)
      throws AccumuloSecurityException {
    ZooReaderWriter zoo = context.getZooSession().asReaderWriter();

    // create the root user with no record-level authorizations
    try {
      // prep parent node of users with root username
      if (!zoo.exists(zkUserPath)) {
        zoo.putPersistentData(zkUserPath, rootuser.getBytes(UTF_8), NodeExistsPolicy.FAIL);
      }

      initUser(rootuser);
      zoo.putPersistentData(zkUserPath + "/" + rootuser + ZKUserAuths,
          ZKSecurityTool.convertAuthorizations(Authorizations.EMPTY), NodeExistsPolicy.FAIL);
    } catch (KeeperException | InterruptedException e) {
      log.error("{}", e.getMessage(), e);
      throw new IllegalStateException(e);
    }
  }

  @Override
  public void initUser(String user) throws AccumuloSecurityException {
    ZooReaderWriter zoo = context.getZooSession().asReaderWriter();
    try {
      zoo.putPersistentData(zkUserPath + "/" + user, new byte[0], NodeExistsPolicy.SKIP);
    } catch (KeeperException e) {
      log.error("{}", e.getMessage(), e);
      throw new AccumuloSecurityException(user, SecurityErrorCode.CONNECTION_ERROR, e);
    } catch (InterruptedException e) {
      log.error("{}", e.getMessage(), e);
      throw new IllegalStateException(e);
    }
  }

  @Override
  public void dropUser(String user) throws AccumuloSecurityException {
    try {
      context.getZooSession().asReaderWriter()
          .recursiveDelete(zkUserPath + "/" + user + ZKUserAuths, NodeMissingPolicy.SKIP);
      context.getZooCache().clear((path) -> path.startsWith(zkUserPath + "/" + user));
    } catch (InterruptedException e) {
      log.error("{}", e.getMessage(), e);
      throw new IllegalStateException(e);
    } catch (KeeperException e) {
      log.error("{}", e.getMessage(), e);
      if (e.code().equals(KeeperException.Code.NONODE)) {
        throw new AccumuloSecurityException(user, SecurityErrorCode.USER_DOESNT_EXIST, e);
      }
      throw new AccumuloSecurityException(user, SecurityErrorCode.CONNECTION_ERROR, e);

    }
  }

  @Override
  public void changeAuthorizations(String user, Authorizations authorizations)
      throws AccumuloSecurityException {
    try {
      String userAuths = zkUserPath + "/" + user + ZKUserAuths;
      context.getZooCache().clear(userAuths);
      context.getZooSession().asReaderWriter().putPersistentData(userAuths,
          ZKSecurityTool.convertAuthorizations(authorizations), NodeExistsPolicy.OVERWRITE);
    } catch (KeeperException e) {
      log.error("{}", e.getMessage(), e);
      throw new AccumuloSecurityException(user, SecurityErrorCode.CONNECTION_ERROR, e);
    } catch (InterruptedException e) {
      log.error("{}", e.getMessage(), e);
      throw new IllegalStateException(e);
    }
  }

  @Override
  public boolean isValidAuthorizations(String user, List<ByteBuffer> auths) {
    if (auths.isEmpty()) {
      // avoid deserializing auths from ZK cache
      return true;
    }

    Authorizations userauths = getCachedUserAuthorizations(user);

    for (ByteBuffer auth : auths) {
      if (!userauths.contains(ByteBufferUtil.toBytes(auth))) {
        return false;
      }
    }

    return true;
  }
}
