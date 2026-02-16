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
        t.v,
		t.unit
    FROM trades AS t
    LEFT JOIN LATERAL (
	    SELECT r.c
		FROM trades AS r
		WHERE r.asset_id = rate_id
            AND r.agg_type = 'D'
			AND r.dt <= t.dt
		ORDER BY r.dt DESC
		LIMIT 1
    ) AS r ON TRUE;
END;
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
		WHEN t.unit LIKE 'yuan/kg' THEN (t.c * 31.103477 / 1000)
		WHEN t.unit LIKE 'yuan/g' THEN (t.c * 31.103477)
	-- excluding VAT, as SMM itself does when converting from CNY to USD
	END / 1.13)::DECIMAL(20, 4) AS c,
	'USD/oz'::VARCHAR(15) AS unit
FROM trades_reduced_by('SMM', 'SMM-EXR-003') AS t
INNER JOIN assets AS a
	ON a.id = t.asset_id
		AND a.market = 'SMM'
WHERE t.unit IN ('yuan/kg', 'yuan/g');
