package org.springframework.data.couchbase.core;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.transactions.ReactiveTransactionAttemptContext;
import com.couchbase.client.java.transactions.TransactionAttemptContext;
import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;
import org.springframework.lang.Nullable;
import reactor.core.publisher.Mono;

import java.util.Optional;
import java.util.function.Function;

// todo gp better name
@Stability.Internal
class GenericSupportHelper {
    public final CouchbaseDocument converted;
    public final Long cas;
    public final ReactiveCollection collection;
    public final @Nullable ReactiveTransactionAttemptContext ctx;

    public GenericSupportHelper(CouchbaseDocument doc, Long cas, ReactiveCollection collection, @Nullable ReactiveTransactionAttemptContext ctx) {
        this.converted = doc;
        this.cas = cas;
        this.collection = collection;
        this.ctx = ctx;
    }
}

// todo gp better name
@Stability.Internal
public class GenericSupport {
    public static <T> Mono<T> one(Mono<ReactiveCouchbaseTemplate> tmpl,
                                  String scopeName,
                                  String collectionName,
                                  ReactiveTemplateSupport support,
                                  T object,
                                  Function<GenericSupportHelper, Mono<T>> nonTransactional,
                                  Function<GenericSupportHelper, Mono<T>> transactional) {
        // todo gp how safe is this?  I think we can switch threads potentially
        //Optional<TransactionAttemptContext> ctxr = Optional.ofNullable((TransactionAttemptContext)
        //        org.springframework.transaction.support.TransactionSynchronizationManager.getResource(TransactionAttemptContext.class));

        return tmpl.flatMap(template -> template.getCouchbaseClientFactory().withScope(scopeName).getCollection(collectionName)
                .flatMap(collection ->
                        support.encodeEntity(object)
                                  .flatMap(converted -> tmpl.flatMap(tp -> tp.getCouchbaseClientFactory().getSession(null).flatMap(s -> {
                                    GenericSupportHelper gsh = new GenericSupportHelper(converted, support.getCas(object), collection.reactive(), s != null ? s.getReactiveTransactionAttemptContext() : null);
                                    if (s == null || s.getReactiveTransactionAttemptContext() == null) {
                                        return nonTransactional.apply(gsh);
                                    } else {
                                        return transactional.apply(gsh);
                                    }
                                }))
                .onErrorMap(throwable -> {
                    if (throwable instanceof RuntimeException) {
                        return template.potentiallyConvertRuntimeException((RuntimeException) throwable);
                    } else {
                        return throwable;
                    }
                }))));
    }

}
