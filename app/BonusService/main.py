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

app = FastAPI()

# BonusService

# - BonusService GET /api/v1/bonuses/{UserName} - 200 {"balance", "status"} бонусы пользователя
# - BonusService GET /api/v1/history/{UserName} - 200 история пользователей
# - BonusService POST /api/v1/bonuses/reduce, тело {"name", "bonuses"} - 400, 202 {"balance", "status"}, 404
# - BonusService POST /api/v1/bonuses/add, тело {"name", "bonuses"} - 400, 202 {"balance", "status"}, 404
# - BonusService POST /api/v1/bonuses/calculate_price, тело {"name", "price"} - 400, 202 {"paidByMoney", "paidByBonuses"}, 404
# - BonusService POST /api/v1/bonuses/cancel, тело {"name", "ticket"} - 400, 202 {"balance", "status"}, 404
database_url = os.environ["DATABASE_URL"]
# database_url = "postgresql://program:test@localhost:5432/privileges"
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
    query = text("""select * from privilege where id=1""")
    if not session.exec(query).first():
        query = text("""insert into privilege values (1, 'Test Max', 'GOLD', 0), (2, 'aaa', 'GOLD', 800), (3, 'bbb', 'SILVER', 100)""")
        session.exec(query)
        session.commit()

@app.get('/api/v1/bonuses/{user_name}')
def get_bonuses(user_name: str, session: SessionDep) -> PrivilegeDataJSON:
    privilege = session.exec(select(Privilege).where(Privilege.username == user_name)).first()
    if not privilege:
        return JSONResponse(content={"message": "User not found"}, status_code=404)
    return privilege


@app.get('/api/v1/history/{user_name}')
def get_history(user_name: str, session: SessionDep) -> PrivilegeHistoryDataJSON:
    query = select(Privilege).where(Privilege.username == user_name)
    privilege = session.exec(query).first()
    if not privilege:
        return JSONResponse(content={"message": "User not found"}, status_code=404)
    query = select(Privilege, PrivilegeHistory).where(Privilege.username == user_name).join(PrivilegeHistory, Privilege.id == PrivilegeHistory.privilege_id)
    history = session.exec(query).all()
    items = []
    for h in history:
        items.append(HistoryData(date=str(h[1].datetime), ticketUid=str(h[1].ticket_uid), balanceDiff=h[1].balance_diff, operationType=h[1].operation_type))
    return PrivilegeHistoryDataJSON(status=privilege.status, balance=privilege.balance, history=items)
    
        
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request : Request, exc):
    return JSONResponse({"message": "what", "errors": exc.errors()[0]}, status_code=400)

# - BonusService POST /api/v1/bonuses/reduce, тело {"name", "bonuses"} - 400, 202 {"balance", "status"}
@app.post('/api/v1/bonuses/reduce', status_code=202)
def reduce_bonuses(reduce: ChangeBonusesJSON, session: SessionDep) -> PrivilegeDataJSON:
    query = select(Privilege).where(Privilege.username == reduce.name)
    privilege = session.exec(query).first()
    if not privilege:
        return JSONResponse(content={"message": "User not found"}, status_code=404)
    
    new_balance = privilege.balance - reduce.bonuses
    if new_balance < 0:
        new_balance = 0
    
    query = update(Privilege).where(Privilege.id == privilege.id).values(balance=new_balance)
    session.exec(query)
    session.commit()
    session.refresh(privilege)
    session.add(PrivilegeHistory(privilege_id=privilege.id, ticket_uid=uuid.UUID(reduce.ticketUid), datetime=dt.datetime.now(), balance_diff=reduce.bonuses, operation_type='DEBIT_THE_ACCOUNT'))
    session.commit()
    return privilege

# - BonusService POST /api/v1/bonuses/add, тело {"name", "bonuses"} - 400, 202 {"balance", "status"}, 404
@app.post('/api/v1/bonuses/add', status_code=202)
def add_bonuses(add: ChangeBonusesJSON, session: SessionDep) -> PrivilegeDataJSON:
    query = select(Privilege).where(Privilege.username == add.name)
    privilege = session.exec(query).first()
    if not privilege:
        return JSONResponse(content={"message": "User not found"}, status_code=404)
    
    new_balance = privilege.balance + add.bonuses
    query = update(Privilege).where(Privilege.id == privilege.id).values(balance=new_balance)
    session.exec(query)
    session.commit()
    session.refresh(privilege)
    session.add(PrivilegeHistory(privilege_id=privilege.id, ticket_uid=uuid.UUID(add.ticketUid), datetime=dt.datetime.now(), balance_diff=add.bonuses, operation_type='FILL_IN_BALANCE'))
    session.commit()
    return privilege

# - BonusService POST /api/v1/bonuses/calculate_price, тело CalculatePriceJSON - 400, 202 {"paidByMoney", "paidByBonuses"}, 404
@app.post('/api/v1/bonuses/calculate_price', status_code=202)
def calculate_price(calculatePriceJSON: CalculatePriceJSON, session: SessionDep) -> PaymentDataJSON:
    query = select(Privilege).where(Privilege.username == calculatePriceJSON.name)
    privilege = session.exec(query).first()
    
    if not privilege:
        return JSONResponse(content={"message": "User not found"}, status_code=404)
    
    if not calculatePriceJSON.paidFromBalance:
        additional_bonuses = round(0.1 * calculatePriceJSON.price)
        add_bonuses(ChangeBonusesJSON(name=calculatePriceJSON.name, bonuses=additional_bonuses, ticketUid=calculatePriceJSON.ticketUid), session=session)
        return PaymentDataJSON(paidByMoney=calculatePriceJSON.price, paidByBonuses=0)
    
    if privilege.balance >= calculatePriceJSON.price:
        paidByMoney = 0
        paidByBonuses = calculatePriceJSON.price
    else:
        paidByMoney = calculatePriceJSON.price - privilege.balance
        paidByBonuses = privilege.balance
    
    reduce_bonuses(ChangeBonusesJSON(name=calculatePriceJSON.name, bonuses=paidByBonuses, ticketUid=calculatePriceJSON.ticketUid), session=session)
    return PaymentDataJSON(paidByMoney=paidByMoney, paidByBonuses=paidByBonuses)

# - BonusService POST /api/v1/bonuses/cancel, тело {"name", "ticket"} - 400, 202 {"balance", "status"}, 404
@app.post('/api/v1/bonuses/cancel', status_code=202)
def cancel(cancelTicketJSON: CancelTicketJSON, session: SessionDep) -> PrivilegeDataJSON:
    query = select(Privilege, PrivilegeHistory).where(Privilege.username == cancelTicketJSON.name).join(PrivilegeHistory, Privilege.id == PrivilegeHistory.privilege_id).where(PrivilegeHistory.ticket_uid == uuid.UUID(cancelTicketJSON.ticketUid))
    privilege_history = session.exec(query).first()
    if not privilege_history:
        return JSONResponse(content={"message": "User or ticket not found"}, status_code=404)
    
    changeBonusesJSON = ChangeBonusesJSON(name=cancelTicketJSON.name, bonuses=privilege_history[1].balance_diff, ticketUid=cancelTicketJSON.ticketUid)
    if privilege_history[1].operation_type == 'DEBIT_THE_ACCOUNT':
        return add_bonuses(changeBonusesJSON, session=session)
    else:
        return reduce_bonuses(changeBonusesJSON, session=session)


