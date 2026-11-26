CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS companies (
    registration_number text PRIMARY KEY,               
    company_name        text,                           
    nature_of_business  text,
    industry            text,
    state               text,
    is_struck_off       boolean     NOT NULL DEFAULT false,
    source_updated_at   timestamptz NOT NULL,           
    embedding_text      text,                           
    embedding           vector(384),
    embedding_model     text,                           
    last_message_id     text,
    ingested_at         timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS companies_embedding_hnsw
    ON companies USING hnsw (embedding vector_cosine_ops);

CREATE TABLE IF NOT EXISTS dead_letters (
    id          bigserial   PRIMARY KEY,
    message_key text        NOT NULL UNIQUE,            
    reason      text        NOT NULL,                   
    detail      text        NOT NULL,                
    body        text        NOT NULL,
    received_at timestamptz NOT NULL DEFAULT now()
);
