package io.hyperfoil.tools.h5m.server;

import io.hyperfoil.tools.h5m.FreshDb;
import io.hyperfoil.tools.h5m.entity.Role;
import io.hyperfoil.tools.h5m.entity.User;
import io.hyperfoil.tools.h5m.svc.SecurityEnabledProfile;
import io.hyperfoil.tools.h5m.svc.UserService;
import io.quarkus.security.identity.AuthenticationRequestContext;
import io.quarkus.security.identity.SecurityIdentity;
import io.quarkus.security.runtime.QuarkusPrincipal;
import io.quarkus.security.runtime.QuarkusSecurityIdentity;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
@TestProfile(SecurityEnabledProfile.class)
public class AutoProvisioningTest extends FreshDb {

    @Inject
    OidcUserProvisioner provisioner;

    @Inject
    UserService userService;

    private final AuthenticationRequestContext testContext = new AuthenticationRequestContext() {
        @Override
        public Uni<SecurityIdentity> runBlocking(Supplier<SecurityIdentity> supplier) {
            return Uni.createFrom().item(supplier);
        }
    };

    private SecurityIdentity identityFor(String username) {
        return QuarkusSecurityIdentity.builder()
                .setPrincipal(new QuarkusPrincipal(username))
                .build();
    }

    @Test
    void first_user_gets_admin_role() {
        assertEquals(0, userService.count());
        provisioner.augment(identityFor("first-user"), testContext)
                .subscribe().withSubscriber(io.smallrye.mutiny.helpers.test.UniAssertSubscriber.create())
                .awaitItem();

        User user = userService.byUsername("first-user");
        assertNotNull(user);
        assertEquals(Role.ADMIN, user.role);
    }

    @Test
    void subsequent_user_gets_user_role() {
        userService.create("existing-admin", Role.ADMIN);

        provisioner.augment(identityFor("new-user"), testContext)
                .subscribe().withSubscriber(io.smallrye.mutiny.helpers.test.UniAssertSubscriber.create())
                .awaitItem();

        User user = userService.byUsername("new-user");
        assertNotNull(user);
        assertEquals(Role.USER, user.role);
    }

    @Test
    void existing_user_not_duplicated() {
        userService.create("alice", Role.USER);
        assertEquals(1, userService.count());

        provisioner.augment(identityFor("alice"), testContext)
                .subscribe().withSubscriber(io.smallrye.mutiny.helpers.test.UniAssertSubscriber.create())
                .awaitItem();

        assertEquals(1, userService.count());
    }
}
