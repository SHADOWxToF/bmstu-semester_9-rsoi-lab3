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

def test_get_bonuses():
    response = client.get('/api/v1/bonuses/aaa')
    assert response.status_code == 200
    responseJSON = PrivilegeDataJSON(**response.json())
    assert responseJSON.balance == 800
    assert responseJSON.status == 'GOLD'


def test_get_history_1():
    response = client.get('/api/v1/history/aaa')
    assert response.status_code == 200
    responseJSON: PrivilegeHistoryDataJSON = PrivilegeHistoryDataJSON(**response.json())
    assert responseJSON.status == 'GOLD'
    assert responseJSON.balance == 800
    assert responseJSON.history == []

def test_get_history_2():
    response = client.get('/api/v1/history/unknown')
    assert response.status_code == 404
    
def test_reduce_bonuses():
    ticketUid = str(uuid.uuid4())
    change = ChangeBonusesJSON(ticketUid=ticketUid, name='aaa', bonuses=300)
    response = client.post('/api/v1/bonuses/reduce', json=jsonable_encoder(change))
    assert response.status_code == 202
    responseJSON: PrivilegeDataJSON = PrivilegeDataJSON(**response.json())
    assert responseJSON.status == 'GOLD'
    assert responseJSON.balance == 500


def test_add_bonuses():
    ticketUid = str(uuid.uuid4())
    change = ChangeBonusesJSON(ticketUid=ticketUid, name='aaa', bonuses=200)
    response = client.post('/api/v1/bonuses/add', json=jsonable_encoder(change))
    assert response.status_code == 202
    responseJSON: PrivilegeDataJSON = PrivilegeDataJSON(**response.json())
    assert responseJSON.status == 'GOLD'
    assert responseJSON.balance == 700

def test_calculate_price():
    ticketUid = "5dfdbe1c-2041-481f-8d0c-acb08c7566b0"
    calc = CalculatePriceJSON(name='aaa', price=500, paidFromBalance=True, ticketUid=ticketUid)
    response = client.post('/api/v1/bonuses/calculate_price', json=jsonable_encoder(calc))
    assert response.status_code == 202
    responseJSON: PaymentDataJSON = PaymentDataJSON(**response.json())
    assert responseJSON.paidByMoney == 0
    assert responseJSON.paidByBonuses == 500

def test_cancel():
    ticketUid = "5dfdbe1c-2041-481f-8d0c-acb08c7566b0"
    calc = CancelTicketJSON(name='aaa', ticketUid=ticketUid)
    response = client.post('/api/v1/bonuses/cancel', json=jsonable_encoder(calc))
    assert response.status_code == 202
    responseJSON: PrivilegeDataJSON = PrivilegeDataJSON(**response.json())
    assert responseJSON.balance == 700
    assert responseJSON.status == 'GOLD'

