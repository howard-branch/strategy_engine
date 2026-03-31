-- Migration: 2026-03-31
-- Add market_indices table for non-equity time-series (VIX, etc.)
-- sourced from Yahoo Finance or other free feeds.
--
-- Safe to re-run: uses IF NOT EXISTS throughout.

CREATE SCHEMA IF NOT EXISTS strategy_engine;

CREATE TABLE IF NOT EXISTS strategy_engine.market_indices (
    symbol       TEXT    NOT NULL,
    trade_date   DATE    NOT NULL,
    open         NUMERIC(20, 6),
    high         NUMERIC(20, 6),
    low          NUMERIC(20, 6),
    close        NUMERIC(20, 6),
    adj_close    NUMERIC(20, 6),
    volume       BIGINT,
    source       TEXT    NOT NULL DEFAULT 'yahoo',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (symbol, trade_date)
);

CREATE INDEX IF NOT EXISTS market_indices_trade_date_idx
    ON strategy_engine.market_indices (trade_date);

CREATE INDEX IF NOT EXISTS market_indices_symbol_date_idx
    ON strategy_engine.market_indices (symbol, trade_date);

COMMENT ON TABLE strategy_engine.market_indices IS
    'Daily OHLCV for market-level indices (VIX, Treasury yields, etc.) '
    'pulled from Yahoo Finance.  Not tied to the instruments table.';

