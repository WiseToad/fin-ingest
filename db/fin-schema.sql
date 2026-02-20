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
    code VARCHAR(30) NOT NULL,
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


CREATE OR REPLACE VIEW metal_trades_smm AS
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
    END / (1 + 0.13))::DECIMAL(20, 4) AS price,
    'USD/oz'::VARCHAR(15) AS unit,
    t.c AS price_orig,
    t.unit AS unit_orig,
    r.rate,
    r.rate_dt
FROM trades AS t
JOIN assets AS a
    ON a.id = t.asset_id
CROSS JOIN (
    SELECT r.id AS rate_id
    FROM assets AS r
    WHERE r.market = 'SMM'
        AND r.code = 'SMM-EXR-003')
CROSS JOIN get_rate(rate_id, t.dt) AS r
WHERE a.market = 'SMM'
    AND a.code ~ '^SMM-(AG|AU)-'
    AND t.unit IN ('yuan/kg', 'yuan/g');


CREATE OR REPLACE VIEW metal_trades_sber AS
WITH trades_agg AS (
    SELECT
        t.asset_id,
        a.market,
        a.code,
        a.name,
        'D'::VARCHAR(15) AS agg_type,
        DATE_TRUNC('day', t.dt::DATE) AS dt,
        AVG(t.c)::DECIMAL(20, 4) AS c,
        t.unit
    FROM trades AS t
    JOIN assets AS a
        ON a.id = t.asset_id
    WHERE a.market = 'SBER'
        AND a.code ~ '^(A99|A98)-'
        AND t.agg_type = 'I'
    GROUP BY 
        t.asset_id,
        a.market,
        a.code,
        a.name,
        DATE_TRUNC('day', t.dt::DATE),
        t.unit
)
SELECT
    NULL::BIGINT AS id,
    t.asset_id,
    t.market,
    t.code,
    t.name,
    t.agg_type,
    t.dt,
    (t.c * 31.103477 / t.unit::DECIMAL / r.rate)::DECIMAL(20, 4) AS price,
    'USD/oz'::VARCHAR(15) AS unit,
    t.c AS price_orig,
    t.unit AS unit_orig,
    r.rate,
    r.rate_dt
FROM trades_agg AS t
CROSS JOIN (
    SELECT r.id AS rate_id
    FROM assets AS r
    WHERE r.market = 'CBR'
        AND r.code = 'R01235')
CROSS JOIN get_rate(rate_id, t.dt) AS r
WHERE t.unit ~ '^[0-9]+$';


CREATE OR REPLACE VIEW metal_trades_misx AS
SELECT
    t.id,
    t.asset_id,
    a.market,
    a.code,
    a.name,
    t.agg_type,
    t.dt,
    (t.c * 31.103477 / r.rate)::DECIMAL(20, 4) AS price,
    'USD/oz'::VARCHAR(15) AS unit,
    t.c AS price_orig,
    t.unit AS unit_orig,
    r.rate,
    r.rate_dt
FROM trades AS t
JOIN assets AS a
    ON a.id = t.asset_id
CROSS JOIN (
    SELECT r.id AS rate_id
    FROM assets AS r
    WHERE r.market = 'CBR'
        AND r.code = 'R01235')
CROSS JOIN get_rate(rate_id, t.dt) AS r
WHERE a.market = 'MISX'
    AND a.code ~ '^(SLVRUB|GLDRUB)_';


CREATE OR REPLACE VIEW metal_trades_xcec AS
SELECT
    t.id,
    t.asset_id,
    a.market,
    a.code,
    a.name,
    t.agg_type,
    t.dt,
    t.c AS price,
    'USD/oz'::VARCHAR(15) AS unit,
    t.c AS price_orig,
    t.unit AS unit_orig,
    NULL::DECIMAL AS rate,
    NULL::TIMESTAMP WITH TIME ZONE AS rate_dt
FROM trades AS t
JOIN assets AS a
    ON a.id = t.asset_id
WHERE a.market = 'XCEC'
    AND t.unit IS NULL;


CREATE OR REPLACE VIEW metal_trades_goznak AS
SELECT
    t.id,
    t.asset_id,
    a.market,
    a.code,
    a.name,
    t.agg_type,
    t.dt,
    (CASE a.unit
        WHEN '31.1' then t.c
        WHEN '15.55' then t.c * 2
        WHEN '7.78' then t.c * 4
        WHEN '3.11' then t.c * 10
        ELSE t.c * 31.103477 / a.unit::DECIMAL
    END / r.rate)::DECIMAL(20, 4) AS price,
    'USD/oz'::VARCHAR(15) AS unit,
    t.c AS price_orig,
    a.unit AS unit_orig,
    r.rate,
    r.rate_dt
FROM trades AS t
JOIN assets AS a
    ON a.id = t.asset_id
CROSS JOIN (
    SELECT r.id AS rate_id
    FROM assets AS r
    WHERE r.market = 'CBR'
        AND r.code = 'R01235')
CROSS JOIN get_rate(rate_id, t.dt) AS r
WHERE a.market = 'GOZNAK'
    AND a.unit ~ '^[0-9.]+$';
