/*
 * Copyright 2021-2022 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.couchbase.core;

import reactor.core.publisher.Mono;

import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;
import org.springframework.data.couchbase.core.mapping.event.CouchbaseMappingEvent;
import org.springframework.data.couchbase.repository.support.TransactionResultHolder;

/**
 *
 * @author Michael Reiche
 */
public interface ReactiveTemplateSupport {

	Mono<CouchbaseDocument> encodeEntity(Object entityToEncode);

	<T> Mono<T> decodeEntity(String id, String source, long cas, Class<T> entityClass, String scope, String collection, TransactionResultHolder txHolder);

	<T> Mono<T> applyResult(T entity, CouchbaseDocument converted, Object id, long cas,
			TransactionResultHolder txResultHolder);

	Long getCas(Object entity);

	String getJavaNameForEntity(Class<?> clazz);

	<T> TransactionResultHolder getTxResultHolder(T source);
}
