CREATE TABLE @cdmDatabaseSchema.location (
            location_id integer NOT NULL,
            address_1 varchar(50) NULL,
            address_2 varchar(50) NULL,
            city varchar(50) NULL,
            state varchar(2) NULL,
            zip varchar(9) NULL,
            county varchar(20) NULL,
            location_source_value varchar(50) NULL );
--HINT DISTRIBUTE ON RANDOM

