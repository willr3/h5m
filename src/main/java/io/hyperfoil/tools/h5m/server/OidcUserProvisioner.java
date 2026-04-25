package io.hyperfoil.tools.h5m.server;

import io.hyperfoil.tools.h5m.entity.Role;
import io.hyperfoil.tools.h5m.svc.UserService;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.quarkus.logging.Log;
import io.quarkus.security.identity.AuthenticationRequestContext;
import io.quarkus.security.identity.SecurityIdentity;
import io.quarkus.security.identity.SecurityIdentityAugmentor;
import io.smallrye.mutiny.Uni;

@ApplicationScoped
@Priority(1)
public class OidcUserProvisioner implements SecurityIdentityAugmentor {

    @Inject
    UserService userService;

    @Override
    public Uni<SecurityIdentity> augment(SecurityIdentity identity, AuthenticationRequestContext context) {
        if (identity.isAnonymous()) {
            return Uni.createFrom().item(identity);
        }
        return context.runBlocking(() -> provisionIfNeeded(identity));
    }

    private SecurityIdentity provisionIfNeeded(SecurityIdentity identity) {
        String username = identity.getPrincipal().getName();
        if (userService.byUsername(username) != null) {
            return identity;
        }
        Role role = userService.count() == 0 ? Role.ADMIN : Role.USER;
        userService.create(username, role);
        Log.infof("Auto-provisioned user %s with role %s", username, role);
        return identity;
    }
}
