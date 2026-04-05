CREATE TABLE IF NOT EXISTS card_location_assignments (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    card_last4 VARCHAR(4) NOT NULL,
    cardholder_name VARCHAR NOT NULL DEFAULT '',
    location_name VARCHAR NOT NULL,
    plaid_account_id VARCHAR,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(user_id, card_last4)
);
