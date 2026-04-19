package io.hyperfoil.tools.h5m.svc;

import io.hyperfoil.tools.h5m.FreshDb;
import io.hyperfoil.tools.h5m.entity.FolderEntity;
import io.hyperfoil.tools.h5m.entity.Role;
import io.hyperfoil.tools.h5m.entity.Team;
import io.hyperfoil.tools.h5m.entity.User;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
@TestProfile(SecurityEnabledProfile.class)
public class AuthorizationServiceTest extends FreshDb {

    @Inject
    AuthorizationService authService;

    @Inject
    TeamService teamService;

    @Inject
    UserService userService;

    @Inject
    FolderService folderService;

    @Test
    void security_is_enabled() {
        assertFalse(authService.isLocalMode());
    }

    @Test
    void isAdmin_returns_true_for_admin_user() {
        userService.create("admin-user", Role.ADMIN);
        assertTrue(authService.isAdmin("admin-user"));
    }

    @Test
    void isAdmin_returns_false_for_regular_user() {
        userService.create("regular", Role.USER);
        assertFalse(authService.isAdmin("regular"));
    }

    @Test
    void isAdmin_returns_false_for_unknown_user() {
        assertFalse(authService.isAdmin("nonexistent"));
    }

    @Test
    @Transactional
    void canModifyFolder_returns_true_for_team_member() {
        long teamId = teamService.create("dev");
        long userId = userService.create("alice", Role.USER);
        teamService.addMember(teamId, userId);

        FolderEntity folder = new FolderEntity();
        folder.name = "test-folder";
        folder.team = Team.findById(teamId);
        folder.persist();

        assertTrue(authService.canModifyFolder("alice", folder));
    }

    @Test
    @Transactional
    void canModifyFolder_returns_false_for_non_member() {
        long teamId = teamService.create("dev");
        userService.create("outsider", Role.USER);

        FolderEntity folder = new FolderEntity();
        folder.name = "test-folder";
        folder.team = Team.findById(teamId);
        folder.persist();

        assertFalse(authService.canModifyFolder("outsider", folder));
    }

    @Test
    @Transactional
    void canModifyFolder_returns_true_for_admin() {
        long teamId = teamService.create("dev");
        userService.create("boss", Role.ADMIN);

        FolderEntity folder = new FolderEntity();
        folder.name = "test-folder";
        folder.team = Team.findById(teamId);
        folder.persist();

        assertTrue(authService.canModifyFolder("boss", folder));
    }

    @Test
    @Transactional
    void canModifyFolder_returns_true_when_folder_has_no_team() {
        userService.create("anyone", Role.USER);

        FolderEntity folder = new FolderEntity();
        folder.name = "legacy-folder";
        folder.persist();

        assertTrue(authService.canModifyFolder("anyone", folder));
    }

    @Test
    @Transactional
    void requireFolderModify_throws_for_non_member() {
        long teamId = teamService.create("dev");
        userService.create("outsider", Role.USER);

        FolderEntity folder = new FolderEntity();
        folder.name = "test-folder";
        folder.team = Team.findById(teamId);
        folder.persist();

        assertThrows(SecurityException.class,
                () -> authService.requireFolderModify("outsider", folder));
    }

    @Test
    void requireAdmin_throws_for_non_admin() {
        userService.create("regular", Role.USER);
        assertThrows(SecurityException.class,
                () -> authService.requireAdmin("regular"));
    }
}
