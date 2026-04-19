package io.hyperfoil.tools.h5m.svc;

import io.hyperfoil.tools.h5m.FreshDb;
import io.hyperfoil.tools.h5m.entity.Role;
import io.hyperfoil.tools.h5m.entity.Team;
import io.hyperfoil.tools.h5m.entity.User;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
public class TeamServiceTest extends FreshDb {

    @Inject
    TeamService teamService;

    @Inject
    UserService userService;

    @Test
    void create_team() {
        long id = teamService.create("test-team");
        assertTrue(id > 0);
        Team team = teamService.byName("test-team");
        assertNotNull(team);
        assertEquals("test-team", team.name);
    }

    @Test
    void list_teams() {
        teamService.create("alpha");
        teamService.create("beta");
        List<Team> teams = teamService.list();
        assertEquals(2, teams.size());
    }

    @Test
    void delete_team() {
        long id = teamService.create("to-delete");
        assertNotNull(teamService.byName("to-delete"));
        teamService.delete(id);
        assertNull(teamService.byName("to-delete"));
    }

    @Test
    @Transactional
    void add_member() {
        long teamId = teamService.create("dev-team");
        long userId = userService.create("alice", Role.USER);
        teamService.addMember(teamId, userId);

        Team team = Team.findById(teamId);
        assertNotNull(team);
        assertEquals(1, team.members.size());
        assertEquals("alice", team.members.get(0).username);
    }

    @Test
    @Transactional
    void remove_member() {
        long teamId = teamService.create("dev-team");
        long userId = userService.create("bob", Role.USER);
        teamService.addMember(teamId, userId);

        Team team = Team.findById(teamId);
        assertEquals(1, team.members.size());

        teamService.removeMember(teamId, userId);
        team = Team.findById(teamId);
        assertEquals(0, team.members.size());
    }

    @Test
    @Transactional
    void add_member_is_idempotent() {
        long teamId = teamService.create("dev-team");
        long userId = userService.create("carol", Role.USER);
        teamService.addMember(teamId, userId);
        teamService.addMember(teamId, userId);

        Team team = Team.findById(teamId);
        assertEquals(1, team.members.size());
    }
}
