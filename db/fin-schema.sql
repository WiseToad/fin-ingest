CREATE OR REPLACE FUNCTION on_update()
RETURNS TRIGGER LANGUAGE 'plpgsql' AS $$
BEGIN
    NEW.updated = NOW();
    RETURN NEW;
END;
$$;


CREATE TABLE assets (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    updated TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    market VARCHAR(15) NOT NULL,
    code VARCHAR(25) NOT NULL,
    name TEXT NOT NULL,
    unit VARCHAR(15));

ALTER TABLE assets ADD CONSTRAINT assets_uk_01 UNIQUE NULLS NOT DISTINCT (market, code);

CREATE TRIGGER on_assets_update
BEFORE UPDATE ON assets FOR EACH ROW
EXECUTE FUNCTION on_update();


CREATE TABLE trades (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    updated TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    asset_id BIGINT NOT NULL CONSTRAINT trades_fk_01 REFERENCES assets(id),
    agg_type VARCHAR(15) NOT NULL,
    dt TIMESTAMP WITH TIME ZONE NOT NULL,
    o DECIMAL(20, 4),
    h DECIMAL(20, 4),
    l DECIMAL(20, 4),
    c DECIMAL(20, 4),
    v BIGINT,
    unit VARCHAR(15));

ALTER TABLE trades ADD CONSTRAINT trades_uk_01 UNIQUE (asset_id, agg_type, dt);

CREATE INDEX trades_ix_01 ON trades(related_id);

CREATE TRIGGER on_tradess_update
BEFORE UPDATE ON trades FOR EACH ROW
EXECUTE FUNCTION on_update();


CREATE OR REPLACE FUNCTION trades_reduced_by(market VARCHAR, code VARCHAR)
RETURNS SETOF trades LANGUAGE 'plpgsql' AS $$
DECLARE
    rate_id assets.id%TYPE;
BEGIN
    SELECT a.id FROM assets AS a
    INTO rate_id
    WHERE a.market = trades_reduced_by.market
        AND a.code = trades_reduced_by.code;

    IF rate_id IS NULL THEN
        RAISE EXCEPTION 'No rate found of market % and code %', market, code;
    END IF;

    RETURN QUERY
    SELECT
        t.id,
        t.updated,
        t.asset_id,
        t.agg_type,
        t.dt,
        (t.o / r.c)::DECIMAL(20, 4) AS o,
        (t.h / r.c)::DECIMAL(20, 4) AS h,
        (t.l / r.c)::DECIMAL(20, 4) AS l,
        (t.c / r.c)::DECIMAL(20, 4) AS c,
        t.v
    FROM trades AS t
    LEFT JOIN trades AS r
        ON r.asset_id = rate_id
            AND r.agg_type = 'D'
            AND r.dt::DATE = t.dt::DATE;
END;
$$;


CREATE OR REPLACE VIEW trades_smm AS
SELECT
    t.id,
    t.asset_id,
    a.market,
    a.code AS asset_code,
    a.name AS asset_name,
    'USD/oz'::VARCHAR(15) AS asset_unit,
    t.agg_type,
    t.dt,
    (CASE
        WHEN a.unit LIKE 'yuan/kg' THEN (t.c * 31.1 / 1000)
        WHEN a.unit LIKE 'yuan/g' THEN (t.c * 31.1)
    END / 1.13)::DECIMAL(20, 4) AS c_no_vat
FROM trades_reduced_by('SMM', 'SMM-EXR-003') AS t
INNER JOIN assets AS a
    ON a.id = t.asset_id
        AND a.market = 'SMM'
        AND a.unit IN ('yuan/kg', 'yuan/g');
