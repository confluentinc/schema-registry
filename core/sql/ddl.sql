DROP TABLE IF EXISTS refs CASCADE;
DROP TABLE IF EXISTS schemas CASCADE;
DROP TABLE IF EXISTS contexts CASCADE;
DROP TABLE IF EXISTS subjects CASCADE;

CREATE TABLE contexts (id SERIAL PRIMARY KEY, tenant text, context text, schemas int);
CREATE UNIQUE INDEX unique_context ON contexts (tenant, context);

CREATE TABLE subjects (id SERIAL PRIMARY KEY, context_id int, subject text);
CREATE UNIQUE INDEX unique_subject ON subjects (context_id, subject);
ALTER TABLE subjects ADD CONSTRAINT context_id_fk FOREIGN KEY (context_id) REFERENCES contexts (id);

CREATE TABLE schemas (
                         id int,
                         subject_id int,
                         version int,
                         type text,
                         str text,
                         hash bytea,
                         deleted bool
);
ALTER TABLE schemas ADD PRIMARY KEY (id, subject_id);
CREATE UNIQUE INDEX unique_schema ON schemas (subject_id, version);
CREATE INDEX hash_idx ON schemas (hash);
ALTER TABLE schemas ADD CONSTRAINT subject_id_fk FOREIGN KEY (subject_id) REFERENCES subjects (id);

CREATE TABLE refs (subject_id int, schema_id int, name text, ref_subject_id int, ref_schema_id int);
ALTER TABLE refs ADD PRIMARY KEY (subject_id, schema_id, name);
