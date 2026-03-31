-- Add the 'name' column returned by the SHARADAR/ACTIONS API
-- (company name associated with the ticker).
-- Safe to re-run: uses IF NOT EXISTS via DO block.

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM   information_schema.columns
        WHERE  table_schema = 'strategy_engine'
        AND    table_name   = 'corporate_actions'
        AND    column_name  = 'name'
    ) THEN
        ALTER TABLE strategy_engine.corporate_actions
            ADD COLUMN name TEXT;
    END IF;
END
$$;

