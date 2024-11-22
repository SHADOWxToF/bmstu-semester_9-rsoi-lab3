from fastapi.testclient import *
from main import app, engine
from sqlmodel import SQLModel
from database import *
from fastapi.encoders import jsonable_encoder
import uuid

client = TestClient(app)

SQLModel.metadata.create_all(engine)

ticketUid = ""

def test_post_ticket():
    flightNumber = "AFL031"
    username = 'aaa'
    tick = TicketDataJSON(username=username, flightNumber=flightNumber, price=1500)
    response = client.post('/api/v1/tickets/', json=jsonable_encoder(tick))
    assert response.status_code == 201
    responseJSON: TicketJSON = TicketJSON(**response.json())
    assert responseJSON.flightNumber == flightNumber
    assert responseJSON.username == username
    assert responseJSON.price == 1500
    assert responseJSON.status == 'PAID'
    global ticketUid
    ticketUid = responseJSON.ticketUid

def test_delete_ticket():
    flightNumber = "AFL031"
    username = 'aaa'
    response = client.put(f'/api/v1/tickets/{ticketUid}')
    assert response.status_code == 201
    responseJSON: TicketJSON = TicketJSON(**response.json())
    assert responseJSON.flightNumber == flightNumber
    assert responseJSON.username == username
    assert responseJSON.price == 1500
    assert responseJSON.ticketUid == ticketUid
    assert responseJSON.status == 'CANCELED'

def test_get_tickets():
    flightNumber = "AFL031"
    username = 'aaa'
    response = client.get('/api/v1/tickets/', params={"user_name": username})
    assert response.status_code == 200
    responseJSON: list[TicketJSON] = [TicketJSON(**ticket) for ticket in response.json()]
    assert len(responseJSON) == 1
    ticket = responseJSON[0]
    assert ticket.flightNumber == flightNumber
    assert ticket.username == username
    assert ticket.price == 1500
    assert ticket.ticketUid == ticketUid
    assert ticket.status == 'CANCELED'

@app.get('/api/v1/tickets/{ticketUid}')
def test_get_one_ticket():
    flightNumber = "AFL031"
    username = 'aaa'
    response = client.get(f'/api/v1/tickets/{ticketUid}')
    assert response.status_code == 200
    responseJSON: TicketJSON = TicketJSON(**response.json())
    assert responseJSON.flightNumber == flightNumber
    assert responseJSON.username == username
    assert responseJSON.price == 1500
    assert responseJSON.ticketUid == ticketUid
    assert responseJSON.status == 'CANCELED'