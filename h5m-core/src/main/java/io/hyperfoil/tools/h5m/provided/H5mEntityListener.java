package io.hyperfoil.tools.h5m.provided;

import io.agroal.api.AgroalDataSource;
import io.hyperfoil.tools.h5m.entity.Node;
import io.hyperfoil.tools.h5m.entity.Value;
import io.quarkus.hibernate.orm.panache.PanacheEntity;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PostPersist;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreRemove;
import org.hibernate.Session;
import org.hibernate.SessionFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

@ApplicationScoped
//@PersistenceUnitExtension
public class H5mEntityListener {

    @Inject
    AgroalDataSource agroalDataSource;

    @Inject
    SessionFactory sessionFactory;



    @PostPersist
    public void onPersist(PanacheEntity entity) {
        String sql = switch (entity){
            case Node n -> "insert into node_edge (parent_id,child_id,idx,depth) values(?,?,0,0)";
            case Value v -> "insert into value_edge (parent_id,child_id,idx,depth) values(?,?,0,0)";
            default -> "";
        };
        if(!sql.isEmpty()){
//            try(Session session = sessionFactory.openSession()){
//                session.beginTransaction();
//                session.createNativeMutationQuery(sql)
//                        .setParameter(1,entity.id)
//                        .setParameter(2,entity.id)
//                        .executeUpdate();
//            }

            try(Connection connection = agroalDataSource.getConnection()){
                try(PreparedStatement statement = connection.prepareStatement(sql)) {
                    statement.setLong(1, entity.id);
                    statement.setLong(2, entity.id);
                    int count = statement.executeUpdate();
                }
            } catch (SQLException e) {
                //TODO log error instead of throwing runtime exception
                throw new RuntimeException(e);
            }
        }else{
            //todo log error
        }
    }

    @PreRemove
    public void onRemove(PanacheEntity entity) {
        String sql = switch (entity){
            case Node n -> "delete from node_edge where child_id=?";
            case Value v -> "delete from value_edge where child_id=?";
            default -> "";
        };
        if(!sql.isEmpty()){

            try(Connection connection = agroalDataSource.getConnection()){
                
                try(PreparedStatement statement = connection.prepareStatement(sql)) {
                    statement.setLong(1, entity.id);
                    int count = statement.executeUpdate();
                }
            } catch (SQLException e) {
                //TODO log error instead of throwing runtime exception
                throw new RuntimeException(e);
            }
        }else{
            //todo log error
        }

    }
}
