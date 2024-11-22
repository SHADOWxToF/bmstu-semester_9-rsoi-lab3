from fastapi.testclient import *
from main import app, engine
from sqlmodel import SQLModel
from database import *
from fastapi.encoders import jsonable_encoder
import uuid

client = TestClient(app)

SQLModel.metadata.create_all(engine)

def test_init():
    client.post('/manage/init')

# - FlightService GET /api/v1/flights - возвращает все рейсы
# - FlightService GET /api/v1/flights/{flightNumber} - возвращает 200 если такой рейс существует

def test_get_flights():
    flightNumber = "AFL031"
    page = 1
    size = 10
    response = client.get(f'/api/v1/flights', params={"page": page, "size": size})
    assert response.status_code == 200
    responseJSON: FlightsResponse = FlightsResponse(**response.json())
    assert responseJSON.page == page
    assert responseJSON.pageSize == 1
    assert responseJSON.totalElements == 1
    assert len(responseJSON.items) == 1
    item = responseJSON.items[0]
    assert item.date == "2021-10-08 20:00"
    assert item.flightNumber == flightNumber
    assert item.fromAirport == "Санкт-Петербург Пулково"
    assert item.toAirport == "Москва Шереметьево"
    assert item.price == 1500

def test_get_flight():
    flightNumber = "AFL031"
    response = client.get(f'/api/v1/flights/{flightNumber}')
    assert response.status_code == 200
    responseJSON: FlightData = FlightData(**response.json())
    assert responseJSON.date == "2021-10-08 20:00"
    assert responseJSON.flightNumber == flightNumber
    assert responseJSON.fromAirport == "Санкт-Петербург Пулково"
    assert responseJSON.toAirport == "Москва Шереметьево"
    assert responseJSON.price == 1500