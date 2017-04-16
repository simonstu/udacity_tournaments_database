-- Table definitions for the tournament project.
--
-- Put your SQL 'create table' statements in this file; also 'create view'
-- statements if you choose to use it.
--
-- You can write comments in this file by starting them with two dashes, like
-- these lines here.

\c tournament;
DROP TABLE IF EXISTS players;
CREATE TABLE players (id serial primary key, name varchar(100));
DROP TABLE IF EXISTS matches;
CREATE TABLE matches (id serial primary key, player1 integer references players(id), player2 integer references players(id), winner integer references players(id));