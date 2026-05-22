package io.hyperfoil.tools.h5m.provided;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.configuration.supplier.AgroalPropertiesReader;
import io.quarkus.agroal.runtime.OpenTelemetryAgroalDataSource;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.enterprise.inject.Default;
import jakarta.enterprise.inject.Disposes;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Named;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

//https://github.com/quarkusio/quarkus/issues/7019

@ApplicationScoped
public class DatasourceConfiguration {

    @ConfigProperty(name = "quarkus.datasource.db-kind",defaultValue = "sqlite")
    String dbKind;
    @ConfigProperty(name = "quarkus.datasource.jdbc.initial-size", defaultValue = "1")
    String initialSize;
    @ConfigProperty(name = "quarkus.datasource.jdbc.min-size", defaultValue = "1")
    String minSize;
    @ConfigProperty(name = "quarkus.datasource.jdbc.max-size", defaultValue = "50")
    String maxSize;
    @ConfigProperty(name = "quarkus.profile")
    String profile;
    @ConfigProperty(name = "quarkus.datasource.username",defaultValue = "quarkus")
    String username;
    @ConfigProperty(name = "quarkus.datasource.password",defaultValue = "quarkus")
    String password;

    //defaultValue empty string does not work for sqlite
    @ConfigProperty(name="quarkus.datasource.jdbc.url",defaultValue = " ")
    String url;


    public DatasourceConfiguration() {
    }

    public static String getPath(){
        String rtrn = "";
        if(System.getenv("H5M_PATH") !=null ){
            rtrn = System.getenv("H5M_PATH");
        } else {
            //check local directory
            Path currentRelativePath = Paths.get("");
            String s = currentRelativePath.toAbsolutePath().toString()+ File.separator+"h5m.db";
            if((new File(s).exists())){
                rtrn = s;
            }else{
                rtrn = System.getProperty("user.home")+File.separator+"h5m.db";
            }
        }
        return rtrn;
    }

    public void initDb(Connection connection) {
        switch(dbKind){
            case "sqlite":
                initSqlite(connection);
                try(Statement statement = connection.createStatement()){
                    //create node_edge before Hibernate so we have the extra column(s)
                    statement.executeUpdate(
                            """
                            CREATE TABLE IF NOT EXISTS node (type varchar(31) not null check ((type in ('ft','sql','user','nata','ecma','rd','root','split','sqlall','fp','jq'))), id bigint not null, multi_type tinyint check ((multi_type between 0 and 1)), name varchar(255), operation TEXT, scalar_method tinyint check ((scalar_method between 0 and 1)), group_id bigint, original_group_id bigint, original_node_id bigint, previous_version_id bigint, target_group_id bigint, primary key (id));
                            CREATE TABLE IF NOT EXISTS node_edge (
                                child_id bigint not null,
                                parent_id bigint not null,
                                idx integer not null,
                                depth int not null default 0,
                                count int not null default 1,
                                primary key (child_id, parent_id, depth), -- removed idx from pkey because idx only matters when depth = 1
                                FOREIGN KEY (child_id) REFERENCES node(id),
                                FOREIGN KEY (parent_id) REFERENCES node(id)
                            );
                            CREATE TRIGGER IF NOT EXISTS new_node AFTER INSERT ON node FOR EACH ROW BEGIN insert into node_edge (parent_id,child_id,idx,depth,count) values(NEW.id,NEW.id,0,0,1); END;
                            CREATE TRIGGER IF NOT EXISTS delete_node_edge_zero_count AFTER UPDATE OF count ON node_edge FOR EACH ROW WHEN NEW.count = 0 BEGIN DELETE FROM node_edge WHERE child_id = NEW.child_id and parent_id = NEW.parent_id; END;
                            
                            CREATE TABLE IF NOT EXISTS value ( id bigint not null, created_at timestamp, data JSONB, idx integer not null, last_updated timestamp, folder_id bigint, node_id bigint, primary key (id) );
                            CREATE INDEX IF NOT EXISTS idx_value_node_id on value (node_id);
                            CREATE INDEX IF NOT EXISTS idx_value_folder_id on value (folder_id);
                            CREATE TABLE IF NOT EXISTS value_edge (
                                child_id bigint not null,
                                parent_id bigint not null,
                                idx integer not null,
                                depth int not null default 0,
                                count int not null default 1,
                                primary key (child_id,parent_id, depth),
                                FOREIGN KEY (child_id) REFERENCES value(id),
                                FOREIGN KEY (parent_id) REFERENCES value(id)
                            );
                            CREATE TRIGGER IF NOT EXISTS new_value AFTER INSERT ON value FOR EACH ROW BEGIN insert into value_edge (parent_id,child_id,idx,depth,count) values(NEW.id,NEW.id,0,0,1); END;
                            CREATE TRIGGER IF NOT EXISTS delete_value_edge_zero_count AFTER UPDATE OF count ON value_edge FOR EACH ROW WHEN NEW.count = 0 BEGIN DELETE FROM value_edge WHERE child_id = NEW.child_id and parent_id = NEW.parent_id; END;
                            """
                    );
                }catch (SQLException e){
                    e.printStackTrace();
                    //org.sqlite.SQLiteException: [SQLITE_ERROR] SQL error or missing database (no such table: node_edge)
                    //org.sqlite.SQLiteException: [SQLITE_ERROR] SQL error or missing database (duplicate column name: depth)
                }

                break;
            case "postgresql":
                initPostgresql(connection);
                try(Statement statement = connection.createStatement()){
                    //create node_edge before Hibernate so we have the extra column(s)
                    statement.executeUpdate(
                            """
                            CREATE TABLE IF NOT EXISTS public.node_group (
                                id bigint NOT NULL,
                                name character varying(255),
                                root_id bigint NOT NULL
                            );
                            CREATE TABLE IF NOT EXISTS public.node (
                                type character varying(31) NOT NULL,
                                id bigint NOT NULL,
                                multi_type smallint,
                                name character varying(255),
                                operation text,
                                scalar_method smallint,
                                group_id bigint,
                                original_group_id bigint,
                                original_node_id bigint,
                                previous_version_id bigint,
                                target_group_id bigint,
                                CONSTRAINT node_multi_type_check CHECK (((multi_type >= 0) AND (multi_type <= 1))),
                                CONSTRAINT node_scalar_method_check CHECK (((scalar_method >= 0) AND (scalar_method <= 1))),
                                CONSTRAINT node_type_check CHECK (((type)::text = ANY ((ARRAY['ecma'::character varying, 'sql'::character varying, 'ft'::character varying, 'split'::character varying, 'fp'::character varying, 'root'::character varying, 'rd'::character varying, 'nata'::character varying, 'sqlall'::character varying, 'jq'::character varying, 'user'::character varying])::text[])))
                            );
                            -- primary keys
                            ALTER TABLE ONLY public.node
                               ADD CONSTRAINT node_pkey PRIMARY KEY (id);
                            ALTER TABLE ONLY public.node_group
                               ADD CONSTRAINT node_group_pkey PRIMARY KEY (id);
                            
                            -- ALTER TABLE ONLY public.node
                            --     ADD CONSTRAINT fk5q83pe2ykwrhttlcrsmavkjnc FOREIGN KEY (original_node_id) REFERENCES public.node(id);
                            -- ALTER TABLE ONLY public.node
                            --     ADD CONSTRAINT fk8rvkseq9cm83glo7ov3h8j3n5 FOREIGN KEY (group_id) REFERENCES public.node_group(id);
                            -- ALTER TABLE ONLY public.node
                            --     ADD CONSTRAINT fkb4xxtogxdms0o3x3aby7b679r FOREIGN KEY (target_group_id) REFERENCES public.node_group(id);
                            -- ALTER TABLE ONLY public.node
                            --     ADD CONSTRAINT fkdgf1fskepiyfuyn83f4sl8r20 FOREIGN KEY (original_group_id) REFERENCES public.node_group(id);
                            -- ALTER TABLE ONLY public.node
                            --     ADD CONSTRAINT fkn7cvmyn1gccd8dtwvvrq5t66h FOREIGN KEY (previous_version_id) REFERENCES public.node(id);
                            
                            
                            -- ALTER TABLE ONLY public.node_group
                            --     ADD CONSTRAINT uk615m1ibxcfjw9yldgh9j5pv8t UNIQUE (root_id);
                            -- ALTER TABLE ONLY public.node_group
                            --     ADD CONSTRAINT fkfuo3mtoqnu9b2avmi6lokag7c FOREIGN KEY (root_id) REFERENCES public.node(id);

                            
                            CREATE TABLE IF NOT EXISTS node_edge (
                                child_id bigint not null,
                                parent_id bigint not null,
                                idx integer not null,
                                depth int not null default 0,
                                count int not null default 1,
                                primary key (child_id, parent_id, depth), -- removed idx from pkey because idx only matters when depth = 1
                                FOREIGN KEY (child_id) REFERENCES node(id),
                                FOREIGN KEY (parent_id) REFERENCES node(id)
                            );
                            create INDEX IF NOT EXISTS idx_node_edge_parent on node_edge (parent_id);
                            create INDEX IF NOT EXISTS idx_node_edge_child on node_edge (child_id);
                            -- CREATE TRIGGER IF NOT EXISTS new_node AFTER INSERT ON node FOR EACH ROW BEGIN insert into node_edge (parent_id,child_id,idx,depth,count) values(NEW.id,NEW.id,0,0,1); END;
                            create or replace function new_node_fn()
                                RETURNS TRIGGER AS $$
                                BEGIN
                                insert into node_edge (parent_id,child_id,idx,depth,count) values(NEW.id,NEW.id,0,0,1);
                                RETURN NEW;
                                END;
                            $$ LANGUAGE plpgsql;
                            CREATE or replace TRIGGER new_node_trigger AFTER INSERT ON node FOR EACH ROW EXECUTE FUNCTION new_node_fn();
                                  
                            -- CREATE TRIGGER IF NOT EXISTS delete_node_edge_zero_count AFTER UPDATE OF count ON node_edge FOR EACH ROW WHEN NEW.count = 0 BEGIN DELETE FROM node_edge WHERE child_id = NEW.child_id and parent_id = NEW.parent_id; END;
                            create or replace function delete_node_edge_zero_count_fn()
                                RETURNS TRIGGER AS $$
                                BEGIN
                                DELETE FROM node_edge WHERE child_id = NEW.child_id and parent_id = NEW.parent_id;
                                RETURN NEW;
                                END;
                            $$ LANGUAGE plpgsql;
                            create or replace TRIGGER delete_node_edge_zero_count_trigger AFTER UPDATE on node_edge FOR EACH ROW WHEN (NEW.count = 0) EXECUTE FUNCTION delete_node_edge_zero_count_fn();
                            
                            CREATE TABLE IF NOT EXISTS value ( id bigint not null, created_at timestamp, data JSONB, idx integer not null, last_updated timestamp, folder_id bigint, node_id bigint, primary key (id) );
                            CREATE INDEX IF NOT EXISTS idx_value_node_id on value (node_id);
                            CREATE INDEX IF NOT EXISTS idx_value_folder_id on value (folder_id);
                            CREATE TABLE IF NOT EXISTS value_edge (
                                child_id bigint not null,
                                parent_id bigint not null,
                                idx integer not null,
                                depth int not null default 0,
                                count int not null default 1,
                                primary key (child_id,parent_id, depth),
                                FOREIGN KEY (child_id) REFERENCES value(id),
                                FOREIGN KEY (parent_id) REFERENCES value(id)
                            );
                            CREATE INDEX IF NOT EXISTS idx_value_edge_parent on value_edge (parent_id);
                            -- CREATE TRIGGER IF NOT EXISTS new_value AFTER INSERT ON value FOR EACH ROW BEGIN insert into value_edge (parent_id,child_id,idx,depth,count) values(NEW.id,NEW.id,0,0,1); END;
                            create or replace function new_value_fn()
                                RETURNS TRIGGER AS $$
                                BEGIN
                                insert into value_edge (parent_id,child_id,idx,depth,count) values(NEW.id,NEW.id,0,0,1);
                                RETURN NEW;
                                END;
                            $$ LANGUAGE plpgsql;
                            CREATE or replace TRIGGER new_value_trigger AFTER INSERT ON value FOR EACH ROW EXECUTE FUNCTION new_value_fn();
                            -- CREATE TRIGGER IF NOT EXISTS delete_value_edge_zero_count AFTER UPDATE OF count ON value_edge FOR EACH ROW WHEN NEW.count = 0 BEGIN DELETE FROM value_edge WHERE child_id = NEW.child_id and parent_id = NEW.parent_id; END;
                            create or replace function delete_value_edge_zero_count_fn()
                                RETURNS TRIGGER AS $$
                                BEGIN
                                DELETE FROM value_edge WHERE child_id = NEW.child_id and parent_id = NEW.parent_id;
                                RETURN NEW;
                                END;
                            $$ LANGUAGE plpgsql;
                            create or replace TRIGGER delete_value_edge_zero_count_trigger AFTER UPDATE on value_edge FOR EACH ROW WHEN (NEW.count = 0) EXECUTE FUNCTION delete_value_edge_zero_count_fn();
                            """
                    );
                }catch (SQLException e){
                    e.printStackTrace();
                    //org.sqlite.SQLiteException: [SQLITE_ERROR] SQL error or missing database (no such table: node_edge)
                    //org.sqlite.SQLiteException: [SQLITE_ERROR] SQL error or missing database (duplicate column name: depth)
                }
                break;
        }
        //org.postgresql.util.PSQLException: ERROR: type "tinyint" does not exist: multi_type tinyint, scalar_method tinyint
//        try(Statement statement = connection.createStatement()){
//            statement.executeUpdate(
//                    """
//                    alter table value_edge add column depth int not null default 0;
//                    """
//            );
//        }catch (SQLException e){
//            //likely due to the column already existing
//        }

    }
    public void initSqlite(Connection connection){
        try (Statement statement = connection.createStatement()) {
            //tuning from https://phiresky.github.io/blog/2020/sqlite-performance-tuning/
            statement.executeUpdate(
            """
            pragma journal_mode = WAL;
            pragma synchronous = normal;
            pragma temp_store = memory;
            pragma foreign_keys = on;
            
            """);
/*            try(ResultSet rs = statement.executeQuery("SELECT name FROM sqlite_master")){
                while(rs.next()){
                }
            }
*/
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    public void initPostgresql(Connection connection){}

    public void closeAgroalDataSource(@Disposes AgroalDataSource dataSource){
        if(dbKind.equals("postgresql")){
            dataSource.close();
        }
    }

    @Alternative
    @Produces
    @ApplicationScoped
    @Priority(9999)
    @Default
    @Named("default")
    public AgroalDataSource initDatasource(/*CommandLine.ParseResult parseResult*/) throws SQLException {
        Map<String, String> props = new HashMap<>();
        props.put(AgroalPropertiesReader.MAX_SIZE, maxSize);
        props.put(AgroalPropertiesReader.MIN_SIZE, minSize);
        props.put(AgroalPropertiesReader.INITIAL_SIZE, initialSize);
        props.put(AgroalPropertiesReader.MAX_LIFETIME_S, "57");
        props.put(AgroalPropertiesReader.ACQUISITION_TIMEOUT_S, "54");
        props.put(AgroalPropertiesReader.PRINCIPAL,username); //username
        props.put(AgroalPropertiesReader.CREDENTIAL,password);//password
        if("sqlite".equalsIgnoreCase(dbKind)){
            props.put(AgroalPropertiesReader.PROVIDER_CLASS_NAME , "org.sqlite.JDBC");
            if(!props.containsKey(AgroalPropertiesReader.JDBC_URL)){
                props.put(AgroalPropertiesReader.JDBC_URL, "jdbc:sqlite:"+getPath());
            }
        }else if ("postgresql".equalsIgnoreCase(dbKind)){
            props.put(AgroalPropertiesReader.PROVIDER_CLASS_NAME , "org.postgresql.Driver");
            props.put(AgroalPropertiesReader.JDBC_URL, url );
        }else{

        }


        AgroalDataSource ds  = AgroalDataSource.from(new AgroalPropertiesReader()
                .readProperties(props)
                .get());

        try(Connection connection = ds.getConnection()){
            initDb(connection);
        }
        return ds;
    }

}
