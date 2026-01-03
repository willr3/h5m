-- This file allow to write SQL commands that will be emitted in test and dev.
-- The commands are commented as their support depends of the database
-- insert into myentity (id, field) values(1, 'field-1');
-- insert into myentity (id, field) values(2, 'field-2');
-- insert into myentity (id, field) values(3, 'field-3');
-- alter sequence myentity_seq restart with 4;

-- pragma journal_mode = WAL;
-- pragma synchronous = normal;
-- pragma temp_store = memory;

alter table node_edge add column depth int not null default 0;
create trigger if not exists node_edge_self_reference after insert on node for each row begin insert into node_edge (child_id,parent_id,depth,idx) values (NEW.id,NEW.id,0,0); END;
create table iwashere (foo int);
