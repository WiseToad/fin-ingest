CREATE TABLE accounts (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    broker VARCHAR(15) NOT NULL,
    code VARCHAR(15) NOT NULL,
    name TEXT NOT NULL,
    comment TEXT,
    attrs VARCHAR(15) ARRAY);

ALTER TABLE accounts ADD CONSTRAINT accounts_uk_01 UNIQUE (broker, code);


CREATE TABLE assets (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    market VARCHAR(15) NOT NULL,
    ticker VARCHAR(15) NOT NULL,
    isin VARCHAR(12),
    name TEXT NOT NULL,
    cur VARCHAR(3) NOT NULL);

ALTER TABLE assets ADD CONSTRAINT assets_uk_01 UNIQUE (market, ticker);
ALTER TABLE assets ADD CONSTRAINT assets_uk_02 UNIQUE NULLS DISTINCT (isin);

CREATE TABLE ops (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    broker VARCHAR(15) NOT NULL,
    code VARCHAR(50) NOT NULL,
    corr_id BIGINT CONSTRAINT ops_fk_01 REFERENCES ops(id),
    account_id BIGINT NOT NULL CONSTRAINT ops_fk_02 REFERENCES accounts(id),
    trans_dt TIMESTAMP WITH TIME ZONE NOT NULL,
    settle_dt TIMESTAMP WITH TIME ZONE,
    op_type VARCHAR(15) NOT NULL,
    asset_id BIGINT CONSTRAINT ops_fk_03 REFERENCES assets(id),
    quantity BIGINT,
    amount DECIMAL(20, 4),
    cur VARCHAR(3),
    comment TEXT);

ALTER TABLE ops ADD CONSTRAINT ops_uk_01 UNIQUE NULLS NOT DISTINCT (broker, code);


CREATE OR REPLACE PROCEDURE link_ops_finam()
LANGUAGE 'plpgsql' SECURITY DEFINER AS $$
BEGIN
    UPDATE ops
    SET corr_id = (
        SELECT id FROM ops AS c
        WHERE c.broker = ops.broker
            AND REGEXP_REPLACE(c.code, '^[^-]+-', '') = REGEXP_REPLACE(ops.code, '^[^-]+-', '')
            AND c.code != ops.code
    )
    WHERE ops.broker = 'FINAM'
        AND ops.op_type = 'TRANSFER';
END;
$$;
