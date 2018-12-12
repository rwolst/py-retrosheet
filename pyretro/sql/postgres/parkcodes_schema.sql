drop view if exists vw_home_parks;
drop table if exists parkcodes;

CREATE TABLE parkcodes (
	park_id text not null primary key,
	name text,
	aka text,
	city text,
	state text,
	start text,
	"end" text,
	league text,
	notes text
);
