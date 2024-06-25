### asqlite

A simple and easy to use async wrapper for `sqlite3`.

This is basically the same as `sqlite3` except you use `async with` and `await` in front of most operations.

```python
import asyncio
import asqlite

async def main():
    async with asqlite.connect('example.db') as conn:
        async with conn.cursor() as cursor:
            # Create table
            await cursor.execute('''CREATE TABLE stocks
                                    (date text, trans text, symbol text, qty real, price real)''')

            # Insert a row of data
            await cursor.execute("INSERT INTO stocks VALUES ('2006-01-05','BUY','RHAT',100,35.14)")

            # Save (commit) the changes
            await conn.commit()

asyncio.run(main())
```

### Install

To install this via PyPI, use the `asqlite` package:

```
python3 -m pip install -U asqlite
```

### Differences from `sqlite3`

This module differs from `sqlite3` in a few ways:

1. Connections are created with `journal_mode` set to `wal`.
2. Connections have foreign keys enabled by default.
3. [Implicit transactions are turned off][implicit-transactions]
4. The [`row_factory`][row_factory] is set to [`sqlite3.Row`][Row].
5. A `asqlite.Pool` is provided for connection pooling

[implicit-transactions]: https://docs.python.org/3/library/sqlite3.html#transaction-control
[row_factory]: https://docs.python.org/3/library/sqlite3.html#sqlite3.Connection.row_factory
[Row]: https://docs.python.org/3/library/sqlite3.html#sqlite3.Row

### License

MIT
