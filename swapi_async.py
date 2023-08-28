import asyncio
import aiohttp
from more_itertools import chunked
from models import engine, Session, Base, SwapiPeople

BASE_URL = "https://swapi.dev/api/people/"


async def get_total_count():
    session = aiohttp.ClientSession()
    response = await session.get(BASE_URL)
    json_data = await response.json()
    await session.close()
    total_count = int(json_data["count"] + 1)
    return total_count


async def get_people(people_id: int):
    session = aiohttp.ClientSession()
    response = await session.get(f"https://swapi.dev/api/people/{people_id}")
    json_data = await response.json()
    await session.close()
    return json_data


async def paste_to_db(persons_json):
    async with Session() as session:
        orm_objects = []
        for person in persons_json:
            if "detail" not in person.keys():
                new_person = SwapiPeople(
                    name=person.get("name"),
                    birth_year=person.get("birth_year"),
                    eye_color=person.get("eye_color"),
                    films=",".join(person.get("films")),
                    gender=person.get("gender"),
                    hair_color=person.get("hair_color"),
                    height=person.get("height"),
                    homeworld=person.get("homeworld"),
                    mass=person.get("mass"),
                    skin_color=person.get("skin_color"),
                    species=",".join(person.get("species")),
                    starships=",".join(person.get("starships")),
                    vehicles=",".join(person.get("vehicles")),
                )
                orm_objects.append(new_person)
        session.add_all(orm_objects)
        await session.commit()


async def main():
    count = await get_total_count()
    async with engine.begin() as con:
        await con.run_sync(Base.metadata.create_all)

    persons_coros = (get_people(i) for i in range(1, count))
    person_coros_chunked = chunked(persons_coros, 10)

    for person_coros_chunk in person_coros_chunked:
        persons = await asyncio.gather(*person_coros_chunk)
        asyncio.create_task(paste_to_db(persons))
    tasks = asyncio.all_tasks() - {
        asyncio.current_task(),
    }
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
