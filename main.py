import os
import asyncpg
import importlib
import argparse
import asyncio
import logging
from typing import List, Union

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DSN = os.environ.get("SMOLMIGRATE_DSN")

async def migrations_init():
    """
    Initialize the migrations directory and database table.

    This function creates a 'pg_migrations' directory if it doesn't exist
    and initializes a 'pg_migrations' table in the database.
    """
    directories = os.listdir()
    if "pg_migrations" not in directories:
        os.makedirs("pg_migrations")
        logger.info("Created pg_migrations directory.")
        await run_pg_query("""
            CREATE TABLE IF NOT EXISTS pg_migrations (
                id SERIAL PRIMARY KEY,
                filename TEXT NOT NULL,
                applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        logger.info("Initialized migrations table in the database.")
    else:
        logger.info("A pg_migrations process has already been initialized")

async def check_pg_migrations_exists() -> bool:
    """
    Check if the pg_migrations table exists in the database.

    Returns:
        bool: True if the table exists, False otherwise.
    """
    try:
        result = await run_pg_query("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public'
                AND table_name = 'pg_migrations'
            )
        """)
        return result[0]['exists']
    except Exception as e:
        logger.error(f"Error checking if pg_migrations table exists: {e}")
        return False

async def get_applied_migrations() -> List[str]:
    """
    Retrieve a list of applied migrations from the database.

    Returns:
        List[str]: A list of filenames of applied migrations.
    """
    applied_migrations = await run_pg_query("SELECT filename FROM pg_migrations ORDER BY id")
    return [row['filename'] for row in applied_migrations]

async def add_migration(filename: str, sql: str):
    """
    Add a new migration to the database and create a corresponding file.

    Args:
        filename (str): The name of the migration file.
        sql (str): The SQL content of the migration.
    """
    if not os.path.exists("pg_migrations"):
        logger.error("Run migrations init!")
        return

    try:
        with open(f"pg_migrations/{filename}.py", "w+") as file:
            file.write(f"up_sql = \"\"\"{sql}\"\"\"")
        
        await run_pg_query(sql)
        await run_pg_query("INSERT INTO pg_migrations (filename) VALUES ($1)", filename)
        
        logger.info(f"Migration {filename} applied successfully")
    except Exception as e:
        logger.error(f"Could not apply migration {filename}: {e}")
        os.remove(f"pg_migrations/{filename}.py")

async def pg_metadata_init():
    """
    Initialize the pg_migrations table if it doesn't exist.
    """
    try:
        if not await check_pg_migrations_exists():
            await run_pg_query("""
                    CREATE TABLE IF NOT EXISTS pg_migrations (
                        id SERIAL PRIMARY KEY,
                        filename TEXT NOT NULL,
                        applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
            logger.info("Initialized pg_migrations table")
    except Exception as e:
        logger.error(f"Failed to init pg_metadata: {e}")

async def apply_pending_migrations():
    """
    Apply all pending migrations found in the pg_migrations directory.
    """
    if not os.path.exists("pg_migrations"):
        logger.error("Run migrations init!")
        return

    await pg_metadata_init()
    
    applied_migrations = await get_applied_migrations()
    all_migrations = sorted([f[:-3] for f in os.listdir("pg_migrations") if f.endswith(".py") and f != "__init__.py"])
    
    for migration_file in all_migrations:
        if migration_file not in applied_migrations:
            module_name = f"pg_migrations.{migration_file}"
            migration_module = importlib.import_module(module_name)
            
            logger.info(f"Applying migration: {migration_file}")
            await add_migration(migration_file, migration_module.up_sql)

async def run_pg_query(query: str, *args) -> List[asyncpg.Record]:
    """
    Execute a PostgreSQL query.

    Args:
        query (str): The SQL query to execute.
        *args: Additional arguments for the query.

    Returns:
        List[asyncpg.Record]: The result of the query.

    Raises:
        ValueError: If the POSTGRES_DSN environment variable is not set.
    """
    if not DSN:
        raise ValueError("POSTGRES_DSN environment variable is not set")
    async with asyncpg.create_pool(DSN) as pool:
        async with pool.acquire() as connection:
            if args:
                return await connection.fetch(query, *args)
            else:
                return await connection.fetch(query)

async def create_migration(name: str):
    """
    Create a new migration file.

    Args:
        name (str): The name of the migration.
    """
    if not os.path.exists("pg_migrations"):
        logger.error("Run migrations init!")
        return

    await pg_metadata_init()
    migrations = os.listdir("pg_migrations")
    migration_number = len([f for f in migrations if f.endswith(".py") and f != "__init__.py"]) + 1
    filename = f"{migration_number:03d}_{name}.py"
    
    logger.info("Enter your SQL query (press Enter twice to finish):")
    lines = []
    while True:
        line = input()
        if line:
            lines.append(line)
        else:
            break
    sql_query = "\n".join(lines)
    
    with open(f"pg_migrations/{filename}", "w") as f:
        f.write(f'up_sql = """\n{sql_query}\n"""')
    logger.info(f"Created new migration file: {filename}")

async def list_migrations():
    """
    List all migrations and their status (Applied or Pending).
    """
    if os.path.exists("pg_migrations"):
        all_migrations = sorted([f[:-3] for f in os.listdir("pg_migrations") if f.endswith(".py") and f != "__init__.py"])
    else:
        logger.error("Run migrations init!")
        return

    await pg_metadata_init()
    applied_migrations = await get_applied_migrations()
    logger.info("Migrations:")
    for migration in all_migrations:
        status = "Applied" if migration in applied_migrations else "Pending"
        logger.info(f"  {migration}: {status}")

async def main():
    """
    Main function to handle command-line arguments and execute corresponding actions.
    """
    parser = argparse.ArgumentParser(description="Simple SQL Migration Tool")
    parser.add_argument("command", choices=["init", "migrate", "create", "list"],
                        help="Command to execute")
    parser.add_argument("--name", help="Name for the new migration (used with 'create' command)")

    args = parser.parse_args()

    if args.command == "init":
        await migrations_init()
    elif args.command == "migrate":
        await apply_pending_migrations()
    elif args.command == "create":
        if not args.name:
            logger.error("Error: --name is required for the 'create' command")
            return
        await create_migration(args.name)
    elif args.command == "list":
        await list_migrations()

if __name__ == "__main__":
    asyncio.run(main())