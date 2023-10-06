import asyncio
import aiohttp
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession

# строка подключения к базе данных, формат: 'postgresql+asyncpg://имя_пользователя:пароль@хост/имя_БД'
CONNECTION_STRING = 'postgresql+asyncpg://user:password@localhost/database'

# Создаём структуру и описываем внутри класса Character параметры, что будут храниться внутри таблицы
Base = declarative_base()

class Character(Base):
    __tablename__ = 'characters'

    id = Column(Integer, primary_key=True)
    birth_year = Column(String)
    eye_color = Column(String)
    films = Column(String)
    gender = Column(String)
    hair_color = Column(String)
    height = Column(String)
    homeworld = Column(String)
    mass = Column(String)
    name = Column(String)
    skin_color = Column(String)
    species = Column(String)
    starships = Column(String)
    vehicles = Column(String)

async def fetch_data_from_url(session, url):
    async with session.get(url) as response:
        data = await response.json()
        return data

async def save_character_data(session, aiohttp_session, data, url):
    url_parts = url.split('/')
    character_id = int(url_parts[-2])  # Получаем ID из URL

    character = Character(
        id=character_id,
        birth_year=data.get('birth_year', ''),
        eye_color=data.get('eye_color', ''),
        # для получения имени вместо url будем кидать асинхронный запрос на SWAPI и брать из полученного json формата поле title
        films=', '.join([(await fetch_data_from_url(aiohttp_session, url))["title"] for url in data.get('films', [])]),
        gender=data.get('gender', ''),
        hair_color=data.get('hair_color', ''),
        height=data.get('height', ''),
        homeworld=data.get('homeworld', ''),
        mass=data.get('mass', ''),
        name=data.get('name', ''),
        skin_color=data.get('skin_color', ''),

        # тут аналогично с films, но уже берём поле name
        species=', '.join([(await fetch_data_from_url(aiohttp_session, url))["name"] for url in data.get('species', [])]),
        starships=', '.join([(await fetch_data_from_url(aiohttp_session, url))["name"] for url in data.get('starships', [])]),
        vehicles=', '.join([(await fetch_data_from_url(aiohttp_session, url))["name"] for url in data.get('vehicles', [])])
    )
    # сохранение изменений в таблице
    session.add(character)
    await session.commit()

async def main():
    db_engine = create_async_engine(CONNECTION_STRING, echo=True)
    # коннект к БД
    async with db_engine.begin() as connection:
        async_session = sessionmaker(
            bind=connection,
            expire_on_commit=False,
            class_=AsyncSession
        )

        # сессия для асинхронных запросов на SWAPI
        async with aiohttp.ClientSession() as session:
            count = (await fetch_data_from_url(session, "https://swapi.dev/api/people/"))["count"]
            print(count)
            urls = [f"https://swapi.dev/api/people/{id}/" for id in range(1, count+1)] # результат - список из ссылок формата https://swapi.dev/api/people/ID_персонажа/
            
            for url in urls:
                character_data = await fetch_data_from_url(session, url)
                if character_data == {'detail': 'Not found'}: continue # если записи нету по такому id, то переходим к следующему
                # создание асинхронной сессии для работы с БД
                async with async_session() as db_session:
                    await save_character_data(db_session, session, character_data, url)

if __name__ == "__main__":
    asyncio.run(main())
