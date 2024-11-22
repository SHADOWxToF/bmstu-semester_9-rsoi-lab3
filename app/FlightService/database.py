from sqlmodel import *
import datetime as dt

class FlightData(SQLModel):
    flightNumber: str
    fromAirport: str
    toAirport: str
    date: str
    price: int

class FlightsResponse(SQLModel):
    page: int
    pageSize: int
    totalElements: int
    items: list[FlightData]

class Flight(SQLModel, table=True):
    __tablename__ = "flight"
    id: int = Field(primary_key=True)
    flight_number: str = Field(nullable=False)
    datetime: dt.datetime = Field(sa_column=Column(TIMESTAMP(timezone=True), nullable=False))
    from_airport_id: int = Field(foreign_key="airport.id")
    to_airport_id: int = Field(foreign_key="airport.id")
    price: int = Field(nullable=False)

    def __repr__(self):
        return f"id={self.id}, flight_number={self.flight_number}, datetime={self.datetime}, from_airport_id={self.from_airport_id}, to_airport_id={self.to_airport_id}, price={self.price}"

class Airport(SQLModel, table=True):
    __tablename__ = "airport"
    id: int = Field(primary_key=True)
    name: str = Field()
    city: str = Field()
    country: str = Field()

    def __repr__(self):
        return f"id={self.id}, name={self.name}, city={self.city}, country={self.country}"