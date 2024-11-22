# from sqlalchemy.orm import DeclarativeBase
# from sqlalchemy.orm import *
# from sqlalchemy import *

from fastapi import *
from sqlmodel import *

# - GET api/v1/flights?page=&size=
# ответ

class FlightResponse(SQLModel):
    flightNumber: str
    fromAirport: str
    toAirport: str
    date: str
    price: int


class PaginationResponse(SQLModel):
    page: int
    pageSize: int
    totalElements: int
    items: list[FlightResponse]

# - GET api/v1/tickets
# заголовок X-User-Name
# ответ
# [
class TicketResponse(SQLModel):
    ticketUid: str
    flightNumber: str
    fromAirport: str
    toAirport: str
    date: str
    price: int
    status: str
#   {
#     "ticketUid": "049161bb-badd-4fa8-9d90-87c9a82b0668",
#     "flightNumber": "AFL031",
#     "fromAirport": "Санкт-Петербург Пулково",
#     "toAirport": "Москва Шереметьево",
#     "date": "2021-10-08 20:00",
#     "price": 1500,
#     "status": "PAID"
#   }
# ]

# - POST api/v1/tickets
# заголовок X-User-Name
class TicketPurchaseRequest(SQLModel):
    flightNumber: str
    price: int
    paidFromBalance: bool
# {
#   "flightNumber": "AFL031",
#   "price": 1500,
#   "paidFromBalance": true
# }
# ответ
# 200

class PrivilegeDataJSON(SQLModel):
    balance: int
    status: str


class TicketPurchaseResponse(SQLModel):
    ticketUid: str
    flightNumber: str
    fromAirport: str
    toAirport: str
    date: str
    price: int
    paidByMoney: int
    paidByBonuses: int
    status: str
    privilege: PrivilegeDataJSON | dict
# {
#   "ticketUid": "049161bb-badd-4fa8-9d90-87c9a82b0668",
#   "flightNumber": "AFL031",
#   "fromAirport": "Санкт-Петербург Пулково",
#   "toAirport": "Москва Шереметьево",
#   "date": "2021-10-08 20:00",
#   "price": 1500,
#   "paidByMoney": 1500,
#   "paidByBonuses": 0,
#   "status": "PAID",
#   "privilege": {
#     "balance": 1500,
#     "status": "GOLD"
#   }
# }
# 400
# {
#   "message": "string",
#   "errors": [
#     {
#       "field": "string",
#       "error": "string"
#     }
#   ]
# }

# - GET /api/v1/tickets/{ticketUid}
# заголовок X-User-Name
# query-param X-User-Name
# 200
# {
#   "ticketUid": "049161bb-badd-4fa8-9d90-87c9a82b0668",
#   "flightNumber": "AFL031",
#   "fromAirport": "Санкт-Петербург Пулково",
#   "toAirport": "Москва Шереметьево",
#   "date": "2021-10-08 20:00",
#   "price": 1500,
#   "status": "PAID"
# }
# 404
# {
#   "message": "string"
# }

# - DELETE /api/v1/tickets/{ticketUid}
# заголовок X-User-Name
# query-param X-User-Name
# 204 возврат выполнен
# 404 билет не найден
# {
#   "message": "string"
# }

# - GET /api/v1/me
# заголовок X-User-Name
# 200
class UserInfoResponse(SQLModel):
    tickets: list[TicketResponse]
    privilege: PrivilegeDataJSON | dict
# {
#   "tickets": [
#     {
#       "ticketUid": "049161bb-badd-4fa8-9d90-87c9a82b0668",
#       "flightNumber": "AFL031",
#       "fromAirport": "Санкт-Петербург Пулково",
#       "toAirport": "Москва Шереметьево",
#       "date": "2021-10-08 20:00",
#       "price": 1500,
#       "status": "PAID"
#     }
#   ],
#   "privilege": {
#     "balance": 1500,
#     "status": "GOLD"
#   }
# }

# - GET /api/v1/privilege
# заголовок X-User-Name
# 200
class HistoryData(SQLModel):
    date: str
    ticketUid: str
    balanceDiff: int
    operationType: str


class PrivilegeInfoResponse(SQLModel):
    balance: int
    status: str
    history: list[HistoryData]
# {
#   "balance": 1500,
#   "status": "GOLD",
#   "history": [
#     {
#       "date": "2021-10-08T19:59:19Z",
#       "ticketUid": "049161bb-badd-4fa8-9d90-87c9a82b0668",
#       "balanceDiff": 1500,
#       "operationType": "FILL_IN_BALANCE"
#     }
#   ]
# }

class PrivilegeHistoryDataJSON(SQLModel):
    status: str
    balance: int
    history: list[HistoryData]

class ChangeBonusesJSON(SQLModel):
    ticket_uid: str
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