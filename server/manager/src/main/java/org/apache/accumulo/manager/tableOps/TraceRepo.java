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
package org.apache.accumulo.manager.tableOps;

import static org.apache.accumulo.core.util.LazySingletons.GSON;

import org.apache.accumulo.core.clientImpl.thrift.TInfo;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.manager.Manager;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;

public class TraceRepo<T> implements Repo<T> {

  private static final long serialVersionUID = 1L;

  final TInfo tinfo;
  final Repo<T> repo;

  public TraceRepo(Repo<T> repo) {
    this.repo = repo;
    tinfo = TraceUtil.traceInfo();
  }

  @Override
  public long isReady(FateId fateId, T environment) throws Exception {
    Span span = TraceUtil.startFateSpan(repo.getClass(), repo.getName(), tinfo);
    try (Scope scope = span.makeCurrent()) {
      return repo.isReady(fateId, environment);
    } catch (Exception e) {
      TraceUtil.setException(span, e, true);
      throw e;
    } finally {
      span.end();
    }
  }

  @Override
  public Repo<T> call(FateId fateId, T environment) throws Exception {
    Span span = TraceUtil.startFateSpan(repo.getClass(), repo.getName(), tinfo);
    try (Scope scope = span.makeCurrent()) {
      Repo<T> result = repo.call(fateId, environment);
      if (result == null) {
        return null;
      }
      return new TraceRepo<>(result);
    } catch (Exception e) {
      TraceUtil.setException(span, e, true);
      throw e;
    } finally {
      span.end();
    }
  }

  @Override
  public void undo(FateId fateId, T environment) throws Exception {
    Span span = TraceUtil.startFateSpan(repo.getClass(), repo.getName(), tinfo);
    try (Scope scope = span.makeCurrent()) {
      repo.undo(fateId, environment);
    } catch (Exception e) {
      TraceUtil.setException(span, e, true);
      throw e;
    } finally {
      span.end();
    }
  }

  @Override
  public String getName() {
    return repo.getName();
  }

  @Override
  public String getReturn() {
    return repo.getReturn();
  }

  /**
   * @return string version of Repo that is suitable for logging
   */
  public static String toLogString(Repo<Manager> repo) {
    if (repo instanceof TraceRepo) {
      // There are two reasons the repo is unwrapped. First I could not figure out how to get this
      // to work with Gson. Gson kept serializing nothing for the generic pointer TraceRepo.repo.
      // Second I thought this information was not useful for logging.
      repo = ((TraceRepo<Manager>) repo).repo;
    }

    // Inorder for Gson to work with generic types, the following passes repo.getClass() to Gson.
    // See the Gson javadoc for more info.
    return repo.getClass() + " " + GSON.get().toJson(repo, repo.getClass());
  }
}
