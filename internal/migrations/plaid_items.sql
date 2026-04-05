CREATE TABLE IF NOT EXISTS plaid_items (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    item_id VARCHAR NOT NULL UNIQUE,
    access_token VARCHAR NOT NULL,
    cursor VARCHAR DEFAULT '',
    institution_id VARCHAR,
    institution_name VARCHAR,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Migrate existing token from users table
INSERT INTO plaid_items (user_id, item_id, access_token, cursor, institution_name)
SELECT id, 'legacy-' || id, plaid_access_token, COALESCE(plaid_cursor, ''), 'Regions Bank'
FROM users WHERE plaid_access_token IS NOT NULL AND plaid_access_token != ''
ON CONFLICT (item_id) DO NOTHING;
