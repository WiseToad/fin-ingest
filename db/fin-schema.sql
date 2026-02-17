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

ALTER TABLE trades ADD CONSTRAINT trades_uk_01 UNIQUE NULLS NOT DISTINCT (asset_id, agg_type, dt, unit);

CREATE INDEX trades_ix_01 ON trades(related_id);

CREATE TRIGGER on_tradess_update
BEFORE UPDATE ON trades FOR EACH ROW
EXECUTE FUNCTION on_update();


CREATE OR REPLACE FUNCTION get_rate(id BIGINT, dt TIMESTAMP WITH TIME ZONE)
RETURNS TABLE (
    rate DECIMAL(20, 4),
    rate_dt TIMESTAMP WITH TIME ZONE
)
LANGUAGE sql AS $$
    SELECT r.c, r.dt
    FROM trades AS r
    WHERE r.asset_id = get_rate.id
        AND r.agg_type = 'D'
        AND r.dt <= get_rate.dt
    ORDER BY r.dt DESC
    LIMIT 1;
$$;


CREATE OR REPLACE VIEW smm_trades AS
SELECT
    t.id,
    t.asset_id,
    a.market,
    a.code,
    a.name,
    t.agg_type,
    t.dt,
    (CASE
        WHEN t.unit LIKE 'yuan/kg' THEN (t.c * 31.103477 / 1000 / r.rate)
        WHEN t.unit LIKE 'yuan/g' THEN (t.c * 31.103477 / r.rate)
    -- excluding VAT, as SMM itself does when converting from CNY to USD    
    END / (1 + 0.13))::DECIMAL(20, 4) AS c,
    'USD/oz'::VARCHAR(15) AS unit,
    t.c AS c_orig,
    t.unit AS unit_orig,
    r.rate,
    r.rate_dt
FROM trades AS t
JOIN assets AS a
    ON a.id = t.asset_id
        AND a.market = 'SMM'
CROSS JOIN (
    SELECT r.id AS rate_id
    FROM assets AS r
    WHERE r.market = 'SMM'
        AND r.code = 'SMM-EXR-003')
CROSS JOIN get_rate(rate_id, t.dt) AS r
WHERE t.unit IN ('yuan/kg', 'yuan/g');


CREATE OR REPLACE VIEW sber_trades AS
WITH t AS (
    SELECT
        t.asset_id,
        DATE_TRUNC('day', t.dt::DATE) AS dt,
        AVG(t.c)::DECIMAL(20, 4) AS c,
        t.unit
    FROM trades AS t
    WHERE t.agg_type = 'I'
    GROUP BY 
        t.asset_id,
        DATE_TRUNC('day', t.dt::DATE),
        t.unit
)
SELECT
    NULL::BIGINT AS id,
    t.asset_id,
    a.market,
    a.code,
    a.name,
    'D'::VARCHAR(15) AS agg_type,
    t.dt,
    (t.c * 31.103477 / t.unit::DECIMAL / r.rate)::DECIMAL(20, 4) AS c,
    'USD/oz'::VARCHAR(15) AS unit,
    t.c AS c_orig,
    t.unit AS unit_orig,
    r.rate,
    r.rate_dt
FROM t
JOIN assets AS a
    ON a.id = t.asset_id
        AND a.market = 'SBER'
CROSS JOIN (
    SELECT r.id AS rate_id
    FROM assets AS r
    WHERE r.market = 'CBR'
        AND r.code = 'R01235')
CROSS JOIN get_rate(rate_id, t.dt) AS r
WHERE t.unit ~ '^[0-9]+$';
