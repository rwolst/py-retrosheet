drop table if exists hist_playerids;

CREATE TABLE hist_playerids (
        playerID text not null primary key,
        birthYear text,
        birthMonth text,
        birthDay text,
        birthCountry text,
        birthState text,
        birthCity text,
        deathYear text,
        deathMonth text,
        deathDay text,
        deathCountry text,
        deathState text,
        deathCity text,
        nameFirst text,
        nameLast text,
        nameGiven text,
        weight text,
        height text,
        bats text,
        throws text,
        debut text,
        finalGame text,
        retroID text UNIQUE,
        bbrefID text UNIQUE
);
