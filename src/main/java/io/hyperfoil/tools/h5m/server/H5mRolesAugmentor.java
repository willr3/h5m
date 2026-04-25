package io.hyperfoil.tools.h5m.server;

import io.hyperfoil.tools.h5m.entity.Role;
import io.hyperfoil.tools.h5m.entity.User;
import io.hyperfoil.tools.h5m.svc.UserService;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.quarkus.security.identity.AuthenticationRequestContext;
import io.quarkus.security.identity.SecurityIdentity;
import io.quarkus.security.identity.SecurityIdentityAugmentor;
import io.quarkus.security.runtime.QuarkusSecurityIdentity;
import io.smallrye.mutiny.Uni;

@ApplicationScoped
public class H5mRolesAugmentor implements SecurityIdentityAugmentor {

    @Inject
    UserService userService;

    @Override
    public Uni<SecurityIdentity> augment(SecurityIdentity identity, AuthenticationRequestContext context) {
        if (identity.isAnonymous()) {
            return Uni.createFrom().item(identity);
        }
        return context.runBlocking(() -> addRoles(identity));
    }

    private SecurityIdentity addRoles(SecurityIdentity identity) {
        String username = identity.getPrincipal().getName();
        User user = userService.byUsername(username);
        if (user == null) {
            return identity;
        }
        QuarkusSecurityIdentity.Builder builder = QuarkusSecurityIdentity.builder(identity);
        builder.addRole("user");
        if (user.role == Role.ADMIN) {
            builder.addRole("admin");
        }
        return builder.build();
    }
}
