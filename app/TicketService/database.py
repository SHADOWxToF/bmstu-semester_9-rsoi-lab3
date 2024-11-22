from sqlmodel import *
import datetime as dt
import uuid

class TicketDataJSON(SQLModel):
    username: str
    flightNumber: str
    price: int

class TicketJSON(SQLModel):
    id: int
    ticketUid: str
    username: str
    flightNumber: str
    price: int
    status: str

class Ticket(SQLModel, table=True):
    __tablename__ = "ticket"
    id: int = Field(primary_key=True)
    ticket_uid: uuid.UUID = Field(nullable=False, unique=True)
    username: str = Field(nullable=False)
    flight_number: str = Field(nullable=False)
    price: int = Field(nullable=False)
    status: str = Field(sa_column=Column(String, nullable=False))

    __table_args__ = (CheckConstraint("status in ('PAID', 'CANCELED')"),)
    