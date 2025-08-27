# aioduck: Python asyncio bindings for DuckDB

aioduck provides a friendly, async interface to DuckDB databases.

It replicates the standard `duckdb` module, but with async versions of
all the standard connection and cursor methods, plus context managers
for automatically closing connections and cursors:

``` python
from aioduck import AsyncConnection

async with AsyncConnection(...) as conn:
    await conn.execute("INSERT INTO some_table ...")
    await conn.commit()

    async with conn.cursor() as cursor:
        await cursor.execute("SELECT * FROM some_table")
        async for row in cursor:
            ...
```

It can also be used in the traditional, procedural manner:

``` python
from aioduck import AsyncConnection

db = await AsyncConnection(...)
await db.agent.start()
cursor = db.cursor()
await db.execute('SELECT * FROM some_table')
row = await cursor.fetchone()
rows = await cursor.fetchall()
await cursor.close()
await db.close()
await db.agent.stop()
```

## Install

aioduck is compatible with Python 3.11 and newer. You can install it
from PyPI:

``` console
$ pip install aioduck
```

## Details

aioduck allows interaction with DuckDB databases on the main AsyncIO
event loop without blocking execution of other coroutines while waiting
for queries or data fetches. It does this by using an agent. This agent
executes all actions within a shared request queue to prevent overlapping
actions.

Connection objects are proxies to the real connections through the agent.
Cursors are similarly proxies to the real cursors, and provide async iterators
to query results.

## License

aioduck is licensed under the MIT license. I am providing code in this
repository to you under an open source license. This is my personal repository;
the license you receive to my code is from me and not from my employer. See
the [LICENSE](https://github.com/cnfairydream/aioduck/blob/main/LICENSE)
file for details.

