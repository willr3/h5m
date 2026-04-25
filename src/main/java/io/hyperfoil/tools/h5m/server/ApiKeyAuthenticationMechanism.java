package io.hyperfoil.tools.h5m.server;

import java.util.Collections;
import java.util.Set;

import jakarta.enterprise.context.ApplicationScoped;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.quarkus.security.identity.IdentityProviderManager;
import io.quarkus.security.identity.SecurityIdentity;
import io.quarkus.security.identity.request.AuthenticationRequest;
import io.quarkus.security.identity.request.BaseAuthenticationRequest;
import io.quarkus.security.runtime.QuarkusPrincipal;
import io.quarkus.security.runtime.QuarkusSecurityIdentity;
import io.quarkus.vertx.http.runtime.security.ChallengeData;
import io.quarkus.vertx.http.runtime.security.HttpAuthenticationMechanism;
import io.quarkus.vertx.http.runtime.security.HttpCredentialTransport;
import io.smallrye.mutiny.Uni;
import io.vertx.ext.web.RoutingContext;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class ApiKeyAuthenticationMechanism implements HttpAuthenticationMechanism {

    private static final String BEARER_PREFIX = "Bearer ";
    private static final String H5M_PREFIX = "H5M_";

    @ConfigProperty(name = "h5m.security.enabled", defaultValue = "false")
    boolean securityEnabled;

    @Override
    public Uni<SecurityIdentity> authenticate(RoutingContext context, IdentityProviderManager identityProviderManager) {
        if (!securityEnabled) {
            SecurityIdentity localAdmin = QuarkusSecurityIdentity.builder()
                    .setPrincipal(new QuarkusPrincipal("local"))
                    .addRole("admin")
                    .addRole("user")
                    .build();
            return Uni.createFrom().item(localAdmin);
        }
        String authorization = context.request().headers().get("Authorization");
        if (authorization == null || !authorization.startsWith(BEARER_PREFIX)) {
            return Uni.createFrom().nullItem();
        }
        String token = authorization.substring(BEARER_PREFIX.length());
        if (!token.startsWith(H5M_PREFIX)) {
            return Uni.createFrom().nullItem();
        }
        return identityProviderManager.authenticate(new Request(token));
    }

    @Override
    public Uni<ChallengeData> getChallenge(RoutingContext context) {
        return Uni.createFrom().item(new ChallengeData(HttpResponseStatus.UNAUTHORIZED.code(), null, null));
    }

    @Override
    public Set<Class<? extends AuthenticationRequest>> getCredentialTypes() {
        return Collections.singleton(Request.class);
    }

    @Override
    public Uni<HttpCredentialTransport> getCredentialTransport(RoutingContext context) {
        return Uni.createFrom().item(new HttpCredentialTransport(HttpCredentialTransport.Type.AUTHORIZATION, "Bearer"));
    }

    public static class Request extends BaseAuthenticationRequest implements AuthenticationRequest {

        private final String key;

        public Request(String key) {
            this.key = key;
        }

        public String getKey() {
            return key;
        }
    }
}
