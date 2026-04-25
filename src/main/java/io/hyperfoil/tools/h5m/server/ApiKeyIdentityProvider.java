package io.hyperfoil.tools.h5m.server;

import io.hyperfoil.tools.h5m.entity.User;
import io.hyperfoil.tools.h5m.svc.ApiKeyService;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;

import io.quarkus.security.identity.AuthenticationRequestContext;
import io.quarkus.security.identity.IdentityProvider;
import io.quarkus.security.identity.SecurityIdentity;
import io.quarkus.security.runtime.QuarkusPrincipal;
import io.quarkus.security.runtime.QuarkusSecurityIdentity;
import io.smallrye.mutiny.Uni;

@ApplicationScoped
public class ApiKeyIdentityProvider implements IdentityProvider<ApiKeyAuthenticationMechanism.Request> {

    @Inject
    ApiKeyService apiKeyService;

    @Override
    public Class<ApiKeyAuthenticationMechanism.Request> getRequestType() {
        return ApiKeyAuthenticationMechanism.Request.class;
    }

    @Override
    public Uni<SecurityIdentity> authenticate(ApiKeyAuthenticationMechanism.Request request,
            AuthenticationRequestContext context) {
        return context.runBlocking(() -> identityFromKey(request.getKey()));
    }

    @Transactional
    SecurityIdentity identityFromKey(String key) {
        User user = apiKeyService.validateKey(key);
        if (user == null) {
            return null;
        }
        return QuarkusSecurityIdentity.builder()
                .setPrincipal(new QuarkusPrincipal(user.username))
                .build();
    }
}
