BEGIN;

CREATE TABLE alembic_version (
    version_num VARCHAR(32) NOT NULL, 
    CONSTRAINT alembic_version_pkc PRIMARY KEY (version_num)
);

-- Running upgrade  -> 0001

CREATE TYPE alembic_mood AS ENUM ('sad', 'ok', 'happy');

CREATE TABLE authors (
    id SERIAL NOT NULL, 
    name TEXT NOT NULL, 
    email TEXT, 
    mood alembic_mood, 
    born DATE, 
    score NUMERIC(10, 2) DEFAULT '0', 
    active BOOLEAN DEFAULT true, 
    created TIMESTAMP WITH TIME ZONE DEFAULT now(), 
    PRIMARY KEY (id), 
    CONSTRAINT authors_email_key UNIQUE (email)
);

COMMENT ON TABLE authors IS 'people who write';

INSERT INTO alembic_version (version_num) VALUES ('0001') RETURNING alembic_version.version_num;

-- Running upgrade 0001 -> 0002

CREATE TABLE books (
    id BIGSERIAL NOT NULL, 
    author_id INTEGER NOT NULL, 
    title TEXT NOT NULL, 
    tags TEXT[], 
    meta JSON, 
    uid UUID, 
    cover BYTEA, 
    reading INTERVAL, 
    rating DOUBLE PRECISION, 
    pages INTEGER, 
    published DATE NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT books_author_id_fkey FOREIGN KEY(author_id) REFERENCES authors (id), 
    CONSTRAINT books_rating_check CHECK (rating >= 0), 
    CONSTRAINT books_author_title_key UNIQUE (author_id, title)
);

CREATE INDEX books_published_idx ON books (published);

INSERT INTO authors (name) VALUES ('Pudge');

INSERT INTO authors (name) VALUES ('Lina');

UPDATE alembic_version SET version_num='0002' WHERE alembic_version.version_num = '0001';

-- Running upgrade 0002 -> 0003

ALTER TABLE books ADD COLUMN summary TEXT;

ALTER TABLE books RENAME summary TO blurb;

CREATE INDEX books_blurb_idx ON books (blurb);

COMMENT ON COLUMN books.blurb IS 'back cover';

UPDATE alembic_version SET version_num='0003' WHERE alembic_version.version_num = '0002';

COMMIT;

