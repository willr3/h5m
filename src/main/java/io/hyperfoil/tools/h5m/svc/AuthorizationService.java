package io.hyperfoil.tools.h5m.svc;

import io.hyperfoil.tools.h5m.entity.FolderEntity;
import io.hyperfoil.tools.h5m.entity.Role;
import io.hyperfoil.tools.h5m.entity.User;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class AuthorizationService {

    @ConfigProperty(name = "h5m.security.enabled", defaultValue = "false")
    boolean securityEnabled;

    @Inject
    UserService userService;

    @Inject
    EntityManager em;

    public boolean isLocalMode() {
        return !securityEnabled;
    }

    @Transactional
    public boolean isAdmin(String username) {
        if (isLocalMode()) {
            return true;
        }
        User user = userService.byUsername(username);
        return user != null && user.role == Role.ADMIN;
    }

    @Transactional
    public boolean isMemberOfTeam(String username, long teamId) {
        if (isLocalMode()) {
            return true;
        }
        Long count = em.createQuery(
                "SELECT COUNT(m) FROM team t JOIN t.members m WHERE t.id = :teamId AND m.username = :username",
                Long.class)
                .setParameter("teamId", teamId)
                .setParameter("username", username)
                .getSingleResult();
        return count > 0;
    }

    @Transactional
    public boolean canModifyFolder(String username, FolderEntity folder) {
        if (isLocalMode()) {
            return true;
        }
        if (isAdmin(username)) {
            return true;
        }
        if (folder.team == null) {
            return true; // legacy folder with no team — unrestricted
        }
        return isMemberOfTeam(username, folder.team.id);
    }

    public void requireAdmin(String username) {
        if (!isAdmin(username)) {
            throw new SecurityException("Admin access required");
        }
    }

    public void requireFolderModify(String username, FolderEntity folder) {
        if (!canModifyFolder(username, folder)) {
            throw new SecurityException(
                    "User " + username + " is not a member of team " + folder.team.name);
        }
    }
}
