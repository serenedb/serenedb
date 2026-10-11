INSERT INTO guild (id, name, founded) VALUES (1, 'Radiant', '2003-01-01'), (2, 'Dire', '2003-01-02');

INSERT INTO hero (guild_id, name, level, gold) VALUES
    (1, 'Crystal Maiden', 10, 125.50),
    (2, 'Pudge', 25, 3000),
    (2, 'Lion', 7, 0);

ALTER TABLE hero ADD COLUMN title text;

UPDATE hero SET title = 'Butcher' WHERE name = 'Pudge';
