package io.hyperfoil.tools.h5m.api.svc;

import io.hyperfoil.tools.h5m.entity.ApiKey;

import java.util.List;

public interface ApiKeyServiceInterface {

    String create(String username, String description);

    List<ApiKey> listByUser(String username);

    void revoke(long keyId);
}
