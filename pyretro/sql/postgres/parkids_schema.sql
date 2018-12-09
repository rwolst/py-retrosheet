drop table if exists ParkIDs;

CREATE TABLE parkids (
        key text not null primary key,
        name text not null UNIQUE,
        alias text,
        city text,
        state text,
        country text
);
