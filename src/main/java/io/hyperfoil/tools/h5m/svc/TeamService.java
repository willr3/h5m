package io.hyperfoil.tools.h5m.svc;

import io.hyperfoil.tools.h5m.api.svc.TeamServiceInterface;
import io.hyperfoil.tools.h5m.entity.Team;
import io.hyperfoil.tools.h5m.entity.User;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.transaction.Transactional;

import java.util.List;

@ApplicationScoped
public class TeamService implements TeamServiceInterface {

    @Override
    @Transactional
    public long create(String name) {
        Team team = new Team(name);
        team.persist();
        return team.id;
    }

    @Override
    @Transactional
    public void delete(long teamId) {
        Team.deleteById(teamId);
    }

    @Override
    @Transactional
    public Team byName(String name) {
        return Team.find("name", name).firstResult();
    }

    @Override
    @Transactional
    public List<Team> list() {
        return Team.listAll();
    }

    @Override
    @Transactional
    public void addMember(long teamId, long userId) {
        Team team = Team.findById(teamId);
        User user = User.findById(userId);
        if (team != null && user != null && !team.members.contains(user)) {
            team.members.add(user);
        }
    }

    @Override
    @Transactional
    public void removeMember(long teamId, long userId) {
        Team team = Team.findById(teamId);
        User user = User.findById(userId);
        if (team != null && user != null) {
            team.members.remove(user);
        }
    }
}
