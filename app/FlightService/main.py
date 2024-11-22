from fastapi import *
from fastapi.responses import *
from fastapi.exceptions import RequestValidationError
from sqlmodel import *
from database import *
from typing import Annotated
from fastapi.encoders import jsonable_encoder
from contextlib import asynccontextmanager
import uvicorn
from multiprocessing import Process
import os

app = FastAPI()

# FlightService

# - FlightService GET /api/v1/flights - возвращает все рейсы
# - FlightService GET /api/v1/flights/{flightNumber} - возвращает 200 если такой рейс существует

database_url = os.environ["DATABASE_URL"]
# database_url = "postgresql://program:test@localhost:5432/flights"
print(database_url)
engine = create_engine(database_url)



def create_db_and_tables():
    SQLModel.metadata.create_all(engine)


def get_session():
    with Session(engine) as session:
        yield session


SessionDep = Annotated[Session, Depends(get_session)]


@asynccontextmanager
async def lifespan(app: FastAPI):
    create_db_and_tables()
    yield

app = FastAPI(lifespan=lifespan)


@app.get('/manage/health', status_code=200)
def health():
    return Response(status_code=200)

@app.post('/manage/init')
def init(session: SessionDep):
    query = text("""select * from airport where id=1""")
    if not session.exec(query).first():
        query = text("""insert into airport values
    (1, 'Шереметьево', 'Москва', 'Россия'),
    (2, 'Пулково', 'Санкт-Петербург', 'Россия');
    insert into flight values
    (1, 'AFL031', '2021/10/08 20:00', 2, 1, 1500)
    """)
        session.exec(query)
        session.commit()

# - FlightService GET /api/v1/flights - возвращает все рейсы
# - FlightService GET /api/v1/flights/{flightNumber} - возвращает 200 если такой рейс существует
@app.get('/api/v1/flights')
def get_flights(page: int, size: int, session: SessionDep) -> FlightsResponse:
    query = text(f"""SELECT flight.flight_number, flight.datetime, flight.price, a1.name as n1, a1.city as c1, a2.name as n2, a2.city as c2
from flight join airport a1 on flight.from_airport_id = a1.id join airport a2 on flight.to_airport_id = a2.id""")
    flights = session.exec(query).all()
    items: list[FlightData] = []
    start_index = (page - 1) * size
    end_index = page * size
    for flight in flights[start_index:end_index]:
        fromAirport = flight.c1 + ' ' + flight.n1
        toAirport = flight.c2 + ' ' + flight.n2
        date = flight.datetime.astimezone().strftime("%Y-%m-%d %H:%M")
        items.append(FlightData(flightNumber=flight.flight_number, fromAirport=fromAirport, toAirport=toAirport, date=date, price=flight.price))
    responseBody = FlightsResponse(page=page, pageSize=len(items), totalElements=len(flights), items=items)
    return responseBody


@app.get('/api/v1/flights/{flightNumber}')
def get_flight(flightNumber: str, session: SessionDep) -> FlightData:
    query = text(f"""SELECT flight.flight_number, flight.datetime, flight.price, a1.name as n1, a1.city as c1, a2.name as n2, a2.city as c2
from flight join airport a1 on flight.from_airport_id = a1.id join airport a2 on flight.to_airport_id = a2.id where flight.flight_number=:flight_num""")
    flight = session.exec(query, params={"flight_num": flightNumber}).first()
    if not flight:
        return JSONResponse(content={"message": "Flight not found"}, status_code=404)
    fromAirport = flight.c1 + ' ' + flight.n1
    toAirport = flight.c2 + ' ' + flight.n2
    date = flight.datetime.astimezone().strftime("%Y-%m-%d %H:%M")
    return FlightData(flightNumber=flight.flight_number, fromAirport=fromAirport, toAirport=toAirport, date=date, price=flight.price)
    