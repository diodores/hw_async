import asyncio
import asyncpg
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession


# для красивого вывода
from rich import print


# Коннект к бд
db_engine = create_async_engine('postgresql+asyncpg://user:password@localhost/database', echo=True)

# Создание сессии
Session = sessionmaker(
    bind=db_engine,
    expire_on_commit=False,
    class_=AsyncSession
)

async def retrieve_characters():
    async with Session() as session:
        # SQL-запрос на получение всех данных из таблицы
        query = text('SELECT * FROM characters')
        result = await session.execute(query)

        # Вывод результатов
        for row in result:
            print(row)

async def main():
    await retrieve_characters()

if __name__ == "__main__":
    asyncio.run(main())
