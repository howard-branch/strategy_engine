# Strategy Engine - Setup Guide

## 1. Install Dependencies

Required packages:
- `python-dotenv` — for loading environment variables
- `pandas` — for data handling
- `sqlalchemy` — for database connections
- `psycopg` — PostgreSQL driver

```bash
pip install python-dotenv pandas sqlalchemy psycopg
```

Or use a requirements.txt file (if available).

## 2. Configure Database Credentials

### Option A: Using a `.env` file (Recommended for local development)

1. Copy the example file:
   ```bash
   cp .env.example .env
   ```

2. Edit `.env` with your actual credentials:
   ```
   DB_USER=your_username
   DB_PASSWORD=your_password
   DB_HOST=localhost
   DB_PORT=5432
   DB_NAME=strategy_engine
   DB_SCHEMA=strategy_engine
   ```

3. The `.env` file is **automatically loaded** when you run scripts. It's already in `.gitignore` so credentials won't be committed.

### Option B: Using Environment Variables (For CI/CD, production)

Set environment variables directly:

```bash
export DB_USER=your_username
export DB_PASSWORD=your_password
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=strategy_engine
export DB_SCHEMA=strategy_engine

python experiments/test_load_bars.py
```

### Option C: Using a Full Connection String

Set the `DATABASE_URL` env var (overrides individual settings):

```bash
export DATABASE_URL="postgresql+psycopg://user:password@host:5432/dbname"
python experiments/test_load_bars.py
```

## 3. Run Scripts

Run from the project root:

```bash
# As a module
python -m experiments.test_load_bars

# Direct execution (also works now)
python experiments/test_load_bars.py
```

## 4. Test Configuration

Verify your credentials are loaded correctly:

```bash
python -m config.settings
```

Output:
```
✓ Connection string loaded (masked): postgresql+psycopg://user:***@localhost:5432/strategy_engine
✓ Schema: strategy_engine
```

## 5. Security Notes

- ✅ `.env` files are in `.gitignore` — safe from accidental commits
- ✅ Passwords are **never** hardcoded in scripts
- ✅ Use environment variables in production (Docker, Kubernetes, GitHub Actions, etc.)
- ✅ Never commit `.env` — always use `.env.example` for templates

