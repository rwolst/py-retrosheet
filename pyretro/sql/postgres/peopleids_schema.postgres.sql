drop table if exists PeopleIDs;

CREATE TABLE peopleids (
        key_person text not null,
        key_uuid text not null primary key,
        key_mlbam text UNIQUE,
        key_retro text UNIQUE,
        key_bbref text,
        key_bbref_minors text,
        key_fangraphs text,
        key_npb text,
        key_sr_nfl text,
        key_sr_nba text,
        key_sr_nhl text,
        key_findagrave text,
        name_last text,
        name_first text,
        name_given text,
        name_suffix text,
        name_matrilineal text,
        name_nick text,
        birth_year text,
        birth_month text,
        birth_day text,
        death_year text,
        death_month text,
        death_day text,
        pro_played_first text,
        pro_played_last text,
        mlb_played_first text,
        mlb_played_last text,
        col_played_first text,
        col_played_last text,
        pro_managed_first text,
        pro_managed_last text,
        mlb_managed_first text,
        mlb_managed_last text,
        col_managed_first text,
        col_managed_last text,
        pro_umpired_first text,
        pro_umpired_last text,
        mlb_umpired_first text,
        mlb_umpired_last text
);
