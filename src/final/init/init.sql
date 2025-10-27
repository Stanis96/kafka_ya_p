CREATE TABLE IF NOT EXISTS forbidden_products (
    product_id TEXT PRIMARY KEY,
    reason TEXT,
    added_at TIMESTAMP DEFAULT now()
);
