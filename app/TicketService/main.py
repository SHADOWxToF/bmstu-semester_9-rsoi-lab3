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
import datetime as dt
import uuid

# TicketService

# - TicketService GET /api/v1/tickets?name={UserName}
# - TicketService GET /api/v1/tickets/{ticketUid}, param: ticketUid - 200, 404
# - TicketService POST /api/v1/tickets/, тело {билет} - 202
# - TicketService DELETE /api/v1/tickets/{ticketUid}, param: ticketUid - 201, 404


database_url = os.environ["DATABASE_URL"]
# database_url = "postgresql://program:test@localhost:5432/tickets"
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


# - TicketService GET /api/v1/tickets?name={UserName}
@app.get('/api/v1/tickets/')
def get_tickets(user_name: str, session: SessionDep) -> list[TicketJSON]:
    query = select(Ticket).where(Ticket.username == user_name)
    tickets = session.exec(query).all()

    return [TicketJSON(id=ticket.id, ticketUid=str(ticket.ticket_uid), username=ticket.username, flightNumber=ticket.flight_number, price=ticket.price, status=ticket.status) for ticket in tickets]


# - TicketService GET /api/v1/tickets/{ticketUid}, param: ticketUid - 200, 404
@app.get('/api/v1/tickets/{ticketUid}')
def get_one_ticket(ticketUid: str, session: SessionDep) -> TicketJSON:
    query = select(Ticket).where(Ticket.ticket_uid == ticketUid)
    ticket = session.exec(query).first()
    if not ticket:
        return JSONResponse(content={"message": "Ticket not found"}, status_code=404)
    return TicketJSON(id=ticket.id, ticketUid=str(ticket.ticket_uid), username=ticket.username, flightNumber=ticket.flight_number, price=ticket.price, status=ticket.status)

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request : Request, exc):
    return JSONResponse({"message": "what", "errors": exc.errors()[0]}, status_code=400)

# - TicketService POST /api/v1/tickets/, тело {билет} - 200
@app.post('/api/v1/tickets/', status_code=201)
def post_ticket(newTicket: TicketDataJSON, session: SessionDep) -> TicketJSON:
    ticket = Ticket(username=newTicket.username, ticket_uid=uuid.uuid4(), flight_number=newTicket.flightNumber, price=newTicket.price, status='PAID')
    session.add(ticket)
    session.commit()
    session.refresh(ticket)
    return TicketJSON(id=ticket.id, ticketUid=str(ticket.ticket_uid), username=ticket.username, flightNumber=ticket.flight_number, price=ticket.price, status=ticket.status)

# - TicketService PUT /api/v1/tickets/{ticketUid}, param: ticketUid - 201, 404
@app.put('/api/v1/tickets/{ticketUid}', status_code=201)
def put_ticket(ticketUid: str, session: SessionDep) -> TicketJSON:
    query = select(Ticket).where(Ticket.ticket_uid == ticketUid)
    ticket = session.exec(query).first()
    if not ticket:
        return JSONResponse(content={"message": "Ticket not found"}, status_code=404)
    elif ticket.status == 'CANCELED':
        ticketJSON = TicketJSON(id=ticket.id, ticketUid=str(ticket.ticket_uid), username=ticket.username, flightNumber=ticket.flight_number, price=ticket.price, status=ticket.status)
        return JSONResponse(jsonable_encoder(ticketJSON), status_code=200)
    query = update(Ticket).where(Ticket.ticket_uid == ticketUid).values(status='CANCELED')
    session.exec(query)
    session.commit()
    session.refresh(ticket)
    return TicketJSON(id=ticket.id, ticketUid=str(ticket.ticket_uid), username=ticket.username, flightNumber=ticket.flight_number, price=ticket.price, status=ticket.status)


@app.delete('/api/v1/tickets/{ticketUid}', status_code=202)
def delete_ticket(ticketUid: str, session: SessionDep):
    query = select(Ticket).where(Ticket.ticket_uid == ticketUid)
    ticket = session.exec(query).first()
    if not ticket:
        return JSONResponse(content={"message": "Ticket not found"}, status_code=404)
    query = delete(Ticket).where(Ticket.ticket_uid == ticketUid)
    session.exec(query)
    session.commit()
    return  Response(status_code=202)