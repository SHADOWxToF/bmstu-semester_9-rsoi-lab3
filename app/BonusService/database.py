from sqlmodel import *
import datetime as dt
import uuid

class HistoryData(SQLModel):
    date: str
    ticketUid: str
    balanceDiff: int
    operationType: str

class PrivilegeHistoryDataJSON(SQLModel):
    status: str
    balance: int
    history: list[HistoryData]

class ChangeBonusesJSON(SQLModel):
    ticketUid: str
    name: str
    bonuses: int

class CalculatePriceJSON(SQLModel):
    name: str
    price: int
    paidFromBalance: bool
    ticketUid: str

class CancelTicketJSON(SQLModel):
    name: str
    ticketUid: str

class PaymentDataJSON(SQLModel):
    paidByMoney: int
    paidByBonuses: int

class PrivilegeDataJSON(SQLModel):
    balance: int
    status: str


class Privilege(SQLModel, table=True):
    __tablename__ = "privilege"
    id: int = Field(primary_key=True)
    username: str = Field(nullable=False, unique=True)
    status: str = Field(sa_column=Column(String, nullable=False, default='BRONZE'))
    balance: int
    
    __table_args__ = (CheckConstraint("status in ('BRONZE', 'SILVER', 'GOLD')"),)

class PrivilegeHistory(SQLModel, table=True):
    __tablename__ = "privilege_history"
    id: int = Field(primary_key=True)
    privilege_id: int = Field(foreign_key="privilege.id")
    ticket_uid: uuid.UUID = Field(nullable=False)
    datetime: dt.datetime = Field(sa_column=Column(TIMESTAMP, nullable=False))
    balance_diff: int = Field(nullable=False)
    operation_type: str = Field(sa_column=Column(String, nullable=False))

    __table_args__ = (CheckConstraint("operation_type in ('FILL_IN_BALANCE', 'DEBIT_THE_ACCOUNT')"),)
    