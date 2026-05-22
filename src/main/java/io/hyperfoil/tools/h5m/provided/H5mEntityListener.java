package io.hyperfoil.tools.h5m.provided;

import io.agroal.api.AgroalDataSource;
import io.hyperfoil.tools.h5m.entity.NodeEntity;
import io.hyperfoil.tools.h5m.entity.ValueEntity;
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

    @Inject
    EntityManager em;


    /*
        Runs after the persist but before the txn commits so we couldn't access the node row in the db and were getting constraint violations inserting into node_edge
        There *has to* be a way to insert into node_edge using the new entity.id as part of the same txn...
        Until then we're using triggers on node
     */
    @PostPersist
    public void onPersist(PanacheEntity entity) {


        Session session = em.unwrap(org.hibernate.Session.class);



        String sql = switch (entity){
            case NodeEntity n -> "insert into node_edge (parent_id,child_id,idx,depth,count) values(?,?,0,0,1)";
            case ValueEntity v -> "insert into value_edge (parent_id,child_id,idx,depth) values(?,?,0,0)";
            default -> "";
        };

        if(!sql.isEmpty()){
//            try(Session session = sessionFactory.openSession()){
//                session.createNativeMutationQuery(sql)
//                        .setParameter(1,entity.id)
//                        .setParameter(2,entity.id)
//                        .executeUpdate();
//            }

/*            try(Connection connection = agroalDataSource.getConnection()){
                try(PreparedStatement statement = connection.prepareStatement(sql)) {
                    statement.setLong(1, entity.id);
                    statement.setLong(2, entity.id);
                    long start = System.currentTimeMillis();
                    int count = statement.executeUpdate();
                    long end = System.currentTimeMillis();
                    System.out.println("onPersist "+(end-start)+"ms ->"+count);
                }
            } catch (SQLException e) {
                //TODO log error instead of throwing runtime exception
                throw new RuntimeException(e);
            }*/
        }else{
            //todo log error
        }
    }

    @PreRemove
    public void onRemove(PanacheEntity entity) {
        String sql = switch (entity){
            case NodeEntity n -> "delete from node_edge where child_id=?";
            case ValueEntity v -> "delete from value_edge where child_id=?";
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
