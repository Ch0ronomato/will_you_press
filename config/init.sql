CREATE TYPE bigram AS (w1 text, w2 text);

CREATE TYPE bigrams AS (dilemma bigram[], result bigram[], total bigram[]);

CREATE TABLE questions (
    qid                 integer PRIMARY KEY,
    question_text       text,
    answers             integer,
    yeses               integer,
    questions_bigrams   bigrams
);
