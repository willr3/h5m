package io.hyperfoil.tools.h5m.entity;

import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.ManyToMany;

import java.util.ArrayList;
import java.util.List;

@Entity(name = "h5m_user")
public class User extends PanacheEntityBase {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    public Long id;

    @Column(unique = true)
    public String username;

    @Enumerated(EnumType.STRING)
    public Role role;

    @ManyToMany(mappedBy = "members")
    public List<Team> teams = new ArrayList<>();

    public User() {}

    public User(String username, Role role) {
        this.username = username;
        this.role = role;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof User that)) {
            return false;
        }
        return username.equals(that.username);
    }

    @Override
    public int hashCode() {
        return username.hashCode();
    }

    @Override
    public String toString() {
        return "User<" + id + ">[ username=" + username + " role=" + role + " ]";
    }
}
