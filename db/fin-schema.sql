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


CREATE OR REPLACE FUNCTION to_price_in_oz(price DECIMAL, grams DECIMAL)
RETURNS DECIMAL(20, 4)
LANGUAGE sql AS $$
    SELECT CASE grams
        WHEN 31.1 then price
        WHEN 15.55 then price * 2
        WHEN 7.78 then price * 4
        WHEN 3.11 then price * 10
        ELSE price * 31.103477 / grams
    END::DECIMAL(20, 4)
$$;


CREATE OR REPLACE FUNCTION get_rate(id BIGINT, dt TIMESTAMP WITH TIME ZONE)
RETURNS TABLE (
    rate DECIMAL(20, 4),
    rate_dt TIMESTAMP WITH TIME ZONE
)
LANGUAGE sql AS $$
    SELECT r.c, r.dt
    FROM public.trades AS r
    WHERE r.asset_id = get_rate.id
        AND r.agg_type = 'D'
        AND r.dt <= get_rate.dt
    ORDER BY r.dt DESC
    LIMIT 1;
$$;


CREATE VIEW metal_assets AS
WITH a AS (
    SELECT a.*,
    	CASE a.market
            WHEN 'SMM' THEN
                CASE REGEXP_SUBSTR(a.code, '^SMM-[^-]+')
                    WHEN 'SMM-AU' THEN 'Gold'
                    WHEN 'SMM-AG' THEN 'Silver'
                END
            WHEN 'XCEC' THEN
                CASE REGEXP_SUBSTR(a.name, '(GOLD FUTURES|SILVER FUTURES)')
                    WHEN 'GOLD FUTURES' THEN 'Gold'
                    WHEN 'SILVER FUTURES' THEN 'Silver'
                END
            WHEN 'MISX' THEN
                CASE REGEXP_SUBSTR(a.code, '^[^_]+')
                    WHEN 'GLDRUB' THEN 'Gold'
                    WHEN 'SLVRUB' THEN 'Silver'
                END
            WHEN 'SBER' THEN
                CASE REGEXP_SUBSTR(a.code, '^[^-]+')
                    WHEN 'A98' THEN 'Gold'
                    WHEN 'A99' THEN 'Silver'
                END
            WHEN 'GOZNAK' THEN
                CASE REGEXP_SUBSTR(a.code, '^[^-]+')
                    WHEN 'gold' THEN 'Gold'
                    WHEN 'silver' THEN 'Silver'
                END
        END::VARCHAR(15) AS metal
    FROM public.assets AS a
    WHERE a.market IN ('SMM', 'MISX', 'XCEC', 'SBER', 'GOZNAK')
        AND CASE a.market
            WHEN 'SMM' THEN a.code ~ '^SMM-(AG|AU)-'
            WHEN 'MISX' THEN a.code ~ '^(SLVRUB|GLDRUB)_'
            ELSE TRUE
    	END
)
SELECT id, updated, market, code, name, metal, unit
FROM a WHERE metal IS NOT NULL;


-- Shanghai Metals Market metal trades
CREATE MATERIALIZED VIEW metal_trades_smm AS
SELECT
    t.id,
    t.asset_id,
    a.market,
    a.code,
    a.name,
    a.metal,
    t.agg_type,
    t.dt,
    (to_price_in_oz(t.c,
        CASE t.unit
            WHEN 'yuan/kg' THEN 1000
            WHEN 'yuan/g' THEN 1
        END
    -- excluding VAT, as SMM itself does when converting from CNY to USD    
    ) / r.rate / (1 + 0.13))::DECIMAL(20, 4) AS price,
    'USD/oz'::VARCHAR(15) AS unit,
    t.c AS price_orig,
    t.unit AS unit_orig,
    r.rate,
    r.rate_dt
FROM public.trades AS t
JOIN public.metal_assets AS a
    ON a.id = t.asset_id
CROSS JOIN (
    SELECT r.id AS rate_id
    FROM public.assets AS r
    WHERE r.market = 'SMM'
        AND r.code = 'SMM-EXR-003')
CROSS JOIN public.get_rate(rate_id, t.dt) AS r
WHERE a.market = 'SMM'
    AND t.unit IN ('yuan/kg', 'yuan/g');

-- COMEX metal trades
CREATE MATERIALIZED VIEW metal_trades_xcec AS
SELECT
    t.id,
    t.asset_id,
    a.market,
    a.code,
    a.name,
    a.metal,
    t.agg_type,
    t.dt,
    t.c AS price,
    'USD/oz'::VARCHAR(15) AS unit,
    t.c AS price_orig,
    t.unit AS unit_orig,
    NULL::DECIMAL AS rate,
    NULL::TIMESTAMP WITH TIME ZONE AS rate_dt
FROM public.trades AS t
JOIN public.metal_assets AS a
    ON a.id = t.asset_id
WHERE a.market = 'XCEC'
    AND t.unit IS NULL;

-- Moscow Exchange metal trades
CREATE MATERIALIZED VIEW metal_trades_misx AS
SELECT
    t.id,
    t.asset_id,
    a.market,
    a.code,
    a.name,
    a.metal,
    t.agg_type,
    t.dt,
    (to_price_in_oz(t.c, 1) / r.rate)::DECIMAL(20, 4) AS price,
    'USD/oz'::VARCHAR(15) AS unit,
    t.c AS price_orig,
    t.unit AS unit_orig,
    r.rate,
    r.rate_dt
FROM public.trades AS t
JOIN public.metal_assets AS a
    ON a.id = t.asset_id
CROSS JOIN (
    SELECT r.id AS rate_id
    FROM public.assets AS r
    WHERE r.market = 'CBR'
        AND r.code = 'R01235')
CROSS JOIN public.get_rate(rate_id, t.dt) AS r
WHERE a.market = 'MISX';

-- Sberbank metal trades
CREATE MATERIALIZED VIEW metal_trades_sber AS
WITH trades_agg AS (
    SELECT
        t.asset_id,
        a.market,
        a.code,
        a.name,
        a.metal,
        'D'::VARCHAR(15) AS agg_type,
        DATE_TRUNC('day', t.dt::DATE) AS dt,
        AVG(t.c)::DECIMAL(20, 4) AS c,
        t.unit
    FROM public.trades AS t
    JOIN public.metal_assets AS a
        ON a.id = t.asset_id
    WHERE a.market = 'SBER'
        AND t.agg_type = 'I'
        AND t.c > 0
    GROUP BY 
        t.asset_id,
        a.market,
        a.code,
        a.name,
        a.metal,
        DATE_TRUNC('day', t.dt::DATE),
        t.unit
)
SELECT
    NULL::BIGINT AS id,
    t.asset_id,
    t.market,
    t.code,
    t.name,
    t.metal,
    t.agg_type,
    t.dt,
    (to_price_in_oz(t.c, t.unit::DECIMAL) / r.rate)::DECIMAL(20, 4) AS price,
    'USD/oz'::VARCHAR(15) AS unit,
    t.c AS price_orig,
    t.unit AS unit_orig,
    r.rate,
    r.rate_dt
FROM trades_agg AS t
CROSS JOIN (
    SELECT r.id AS rate_id
    FROM public.assets AS r
    WHERE r.market = 'CBR'
        AND r.code = 'R01235')
CROSS JOIN public.get_rate(rate_id, t.dt) AS r
WHERE t.unit ~ '^[0-9]+$';

-- Goznak metal trades
CREATE MATERIALIZED VIEW metal_trades_goznak AS
SELECT
    t.id,
    t.asset_id,
    a.market,
    a.code,
    a.name,
    a.metal,
    t.agg_type,
    t.dt,
    (to_price_in_oz(t.c, a.unit::DECIMAL) / r.rate)::DECIMAL(20, 4) AS price,
    'USD/oz'::VARCHAR(15) AS unit,
    t.c AS price_orig,
    a.unit AS unit_orig,
    r.rate,
    r.rate_dt
FROM public.trades AS t
JOIN public.metal_assets AS a
    ON a.id = t.asset_id
CROSS JOIN (
    SELECT r.id AS rate_id
    FROM public.assets AS r
    WHERE r.market = 'CBR'
        AND r.code = 'R01235')
CROSS JOIN public.get_rate(rate_id, t.dt) AS r
WHERE a.market = 'GOZNAK'
    AND a.unit ~ '^[0-9.]+$';

-- All metal trades
CREATE VIEW metal_trades AS
SELECT * FROM public.metal_trades_smm
UNION ALL
SELECT * FROM public.metal_trades_xcec
UNION ALL
SELECT * FROM public.metal_trades_misx
UNION ALL
SELECT * FROM public.metal_trades_sber
UNION ALL
SELECT * FROM public.metal_trades_goznak;


-- Latest values
CREATE MATERIALIZED VIEW latest_trades AS
SELECT DISTINCT ON (asset_id, agg_type, unit) *
FROM public.trades AS t
ORDER BY asset_id, agg_type, unit, dt DESC;


-- Latest metal bar trades
CREATE VIEW latest_bar_trades AS
SELECT
    t.id,
    t.asset_id,
    a.market,
    a.code,
    a.name,
    a.metal,
    UPPER(REGEXP_REPLACE(a.code, '^.+-([^-]+)$', '\1')) AS dir,
    t.agg_type,
    t.dt,
    t.c AS price,
    (t.c / COALESCE(t.unit, a.unit)::DECIMAL)::DECIMAL(20, 4) AS price_g,
    COALESCE(t.unit, a.unit)::DECIMAL AS grams
FROM public.latest_trades AS t
JOIN public.metal_assets AS a
    ON a.id = t.asset_id
WHERE a.market IN ('SBER', 'GOZNAK')
    AND COALESCE(t.unit, a.unit) ~ '^[0-9.]+$'
    AND t.c > 0;


CREATE OR REPLACE PROCEDURE refresh_mv()
LANGUAGE 'plpgsql' SECURITY DEFINER AS $$
BEGIN
    REFRESH MATERIALIZED VIEW public.metal_trades_smm;
    REFRESH MATERIALIZED VIEW public.metal_trades_xcec;
    REFRESH MATERIALIZED VIEW public.metal_trades_misx;
    REFRESH MATERIALIZED VIEW public.metal_trades_sber;
    REFRESH MATERIALIZED VIEW public.metal_trades_goznak;
    
    REFRESH MATERIALIZED VIEW public.latest_trades;
END;
$$;
