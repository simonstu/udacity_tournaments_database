#!/usr/bin/env python
#
# tournament.py -- implementation of a Swiss-system tournament
#

import psycopg2


def connect():
    """Connect to the PostgreSQL database.  Returns a database connection."""
    return psycopg2.connect("dbname=tournament")


def deleteMatches():
    """Remove all the match records from the database."""
    db = connect()
    c = db.cursor()
    query = "delete from matches;"
    c.execute(query)
    db.commit()
    db.close()


def deletePlayers():
    """Remove all the player records from the database."""
    db = connect()
    c = db.cursor()
    query = "delete from players;"
    c.execute(query)
    db.commit()
    db.close()


def countPlayers():
    """Returns the number of players currently registered."""
    db = connect()
    c = db.cursor()
    query = "select count(*) as num from players;"
    c.execute(query)
    rows = c.fetchall()
    db.close()
    return rows[0][0]


def registerPlayer(name):
    """Adds a player to the tournament database.

    The database assigns a unique serial id number for the player.  (This
    should be handled by your SQL database schema, not in your Python code.)

    Args:
      name: the player's full name (need not be unique).
    """
    db = connect()
    c = db.cursor()
    query = "INSERT INTO players (name) VALUES (%s);"
    playername = name
    data = (playername,)
    c.execute(query, data)
    db.commit()
    db.close()


def playerStandings():
    """Returns a list of the players and their win records, sorted by wins.

    The first entry in the list should be the player in first place, or a player
    tied for first place if there is currently a tie.

    Returns:
      A list of tuples, each of which contains (id, name, wins, matches):
        id: the player's unique id (assigned by the database)
        name: the player's full name (as registered)
        wins: the number of matches the player has won
        matches: the number of matches the player has played
    """
    db = connect()
    c = db.cursor()

    query = "DROP VIEW IF EXISTS matches_p1 CASCADE"
    c.execute(query)
    db.commit()
    # get all matches by player1
    query = "CREATE VIEW matches_p1 AS SELECT player1 AS player, count(*) AS matches FROM matches GROUP BY player1"
    c.execute(query)
    db.commit()

    query = "DROP VIEW IF EXISTS matches_p2 CASCADE"
    c.execute(query)
    db.commit()
    # get all matches by player2
    query = "CREATE VIEW matches_p2 AS SELECT player2 AS player, count(*) AS matches FROM matches GROUP BY player2"
    c.execute(query)
    db.commit()

    query = "DROP VIEW IF EXISTS matches_by_player CASCADE"
    c.execute(query)
    db.commit()
    # union matches by player1 and player2
    query = "CREATE VIEW matches_by_player AS SELECT * FROM matches_p1 UNION SELECT * FROM matches_p2"
    c.execute(query)
    db.commit()

    query = "DROP VIEW IF EXISTS matches_by_player_aggregated CASCADE"
    c.execute(query)
    db.commit()
    # sum up matches of each player
    query = "CREATE VIEW matches_by_player_aggregated AS SELECT player, sum(matches) AS matches FROM matches_by_player GROUP BY player"
    c.execute(query)
    db.commit()

    query = "DROP VIEW IF EXISTS wins_by_player CASCADE"
    c.execute(query)
    db.commit()
    # get all wins by a player
    query = "CREATE VIEW wins_by_player AS SELECT winner AS player, count(*) AS won_matches FROM matches GROUP BY winner"
    c.execute(query)
    db.commit()

    query = "DROP VIEW IF EXISTS wins_and_matches CASCADE"
    c.execute(query)
    db.commit()
    # join matches and wins
    query = "CREATE VIEW wins_and_matches AS SELECT matches_by_player_aggregated.player, won_matches, matches FROM matches_by_player_aggregated LEFT OUTER JOIN wins_by_player ON (matches_by_player_aggregated.player = wins_by_player.player)"
    c.execute(query)
    db.commit()

    # join names of players
    query = "CREATE VIEW wins_matches_by_player AS SELECT players.id, players.name, coalesce(wins_and_matches.won_matches, 0) AS wins, coalesce(wins_and_matches.matches, 0) FROM players LEFT OUTER JOIN wins_and_matches ON (players.id = wins_and_matches.player)"
    c.execute(query)
    db.commit()
    query = "SELECT * FROM wins_matches_by_player ORDER BY wins"
    c.execute(query)
    db.commit()
    rows = c.fetchall()
    db.close()
    return rows


def reportMatch(winner, loser):
    """Records the outcome of a single match between two players.

    Args:
      winner:  the id number of the player who won
      loser:  the id number of the player who lost
    """

    db = connect()
    c = db.cursor()
    winner_str = winner
    query = "INSERT INTO matches (player1, player2, winner) VALUES (%s, %s, %s);"
    data = (winner, loser, winner)
    c.execute(query, data)
    db.commit()
    db.close()


def swissPairings():
    """Returns a list of pairs of players for the next round of a match.

    Assuming that there are an even number of players registered, each player
    appears exactly once in the pairings.  Each player is paired with another
    player with an equal or nearly-equal win record, that is, a player adjacent
    to him or her in the standings.

    Returns:
      A list of tuples, each of which contains (id1, name1, id2, name2)
        id1: the first player's unique id
        name1: the first player's name
        id2: the second player's unique id
        name2: the second player's name
    """

    all_player = playerStandings()
    pairings=[]
    for index, item in enumerate(all_player):
        if (index%2==0):
            new_pairing = (all_player[index][0], all_player[index][1], all_player[index+1][0], all_player[index+1][1])
            pairings.append(new_pairing)
    return pairings
