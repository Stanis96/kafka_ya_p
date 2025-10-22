CREATE TABLE IF NOT EXISTS products (
    product_id TEXT PRIMARY KEY,
    name TEXT,
    description TEXT,
    price_amount NUMERIC,
    price_currency TEXT,
    category TEXT,
    brand TEXT,
    stock_available INT,
    stock_reserved INT,
    sku TEXT,
    tags TEXT[],
    images JSONB,
    specifications JSONB,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    store_id TEXT
);

CREATE TABLE IF NOT EXISTS forbidden_products (
    product_id TEXT PRIMARY KEY,
    reason TEXT,
    added_at TIMESTAMP DEFAULT now()
);
