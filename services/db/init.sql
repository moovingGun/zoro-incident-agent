CREATE TABLE orders (id serial PRIMARY KEY, item text, created_at timestamptz DEFAULT now());
INSERT INTO orders (item) SELECT 'item-' || g FROM generate_series(1, 200) g;

-- 장애 주입용: delay_ms를 올리면 주문 조회 쿼리가 그만큼 느려진다 (scenario.py db_delay)
CREATE TABLE chaos (delay_ms int NOT NULL);
INSERT INTO chaos VALUES (0);

CREATE FUNCTION recent_orders() RETURNS SETOF orders AS $$
BEGIN
  PERFORM pg_sleep(0.03 + (SELECT delay_ms FROM chaos LIMIT 1) / 1000.0);   -- 평소 쿼리 시간 30ms
  RETURN QUERY SELECT * FROM orders ORDER BY id DESC LIMIT 20;
END
$$ LANGUAGE plpgsql;
