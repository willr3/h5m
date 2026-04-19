package io.hyperfoil.tools.h5m.entity;

import io.quarkus.hibernate.orm.panache.PanacheEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.ManyToOne;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

@Entity(name = "api_key")
public class ApiKey extends PanacheEntity {

    @Column(name = "key_hash")
    public String keyHash; // SHA-256 hex

    @ManyToOne(fetch = FetchType.LAZY)
    public User user;

    public String description;

    public Instant createdAt;

    public Instant lastUsedAt;

    public long activeDays; // number of idle days before the key expires

    public boolean revoked;

    public ApiKey() {}

    public boolean isExpired(Instant now) {
        Instant reference = lastUsedAt != null ? lastUsedAt : createdAt;
        return revoked || (reference != null && now.isAfter(reference.plus(activeDays, ChronoUnit.DAYS)));
    }

    public void recordAccess() {
        this.lastUsedAt = Instant.now();
    }

    @Override
    public String toString() {
        return "ApiKey<" + id + ">[ user=" + (user != null ? user.username : "null") +
                " description=" + description + " ]";
    }
}
