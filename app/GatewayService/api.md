## GET /api/v1/flights

params:

- page
- size

response:

- 200 данные о рейсах 

internal methods:

- FlightService GET /api/v1/flights - возвращает все рейсы

## GET api/v1/tickets

header:

- X-User-Name

response:

- 200 все билеты пользователя

internal methods:

- TicketService GET /api/v1/tickets&name={UserName}
- FlightService GET /api/v1/flights/{flightNumber} - возвращает 200 если такой рейс существует

## POST /api/v1/tickets

header:

- X-User-Name

body:

- flightNumber
- price
- paidFromBalance

response:

- 400 ошибка валидации данных

- 200 данные купленного билета

internal methods:

- FlightService GET /api/v1/flights/{flightNumber} - возвращает 200 если такой рейс существует
- BonusService GET /api/v1/bonuses/{UserName} - 200 {"balance", "status"} бонусы пользователя
- BonusService POST /api/v1/bonuses/calculate_price, тело {"name", "price"} - 400, 202 {"paidByMoney", "paidByBonuses"}
- TicketService POST /api/v1/tickets/, тело {билет} - 202

## GET /api/v1/tickets/{ticketUid}

header:

- X-User-Name

params:

- tickerUid

response:

- 404 не найден билет

- 200 данные билета

internal methods:

- TicketService GET /api/v1/tickets/{ticketUid}, param: ticketUid - 200, 404

## DELETE /api/v1/tickets/{ticketUid}

header:

- X-User-Name

params:

- tickerUid

response:

- 404 билет не найден

- 204 возврат выполнен

internal methods:

- TicketService DELETE /api/v1/tickets/{ticketUid}, param: ticketUid - 204, 404
- BonusService POST /api/v1/bonuses/cancel, тело {"name", "ticket"} - 400, 202 {"balance", "status"}

## GET /api/v1/me

header:

- X-User-Name

response:

- 200 информация о билетах и статусе привелегий

internal methods:

- TicketService GET /api/v1/tickets&name={UserName}
- FlightService GET /api/v1/flights/{flightNumber} - возвращает 200 если такой рейс существует
- BonusService GET /api/v1/bonuses/{UserName} - 200 {"balance", "status"} бонусы пользователя

## GET /api/v1/privilege

header:

- X-User-Name

response:

- 200 данные о бонусном счёте

internal methods:

- TicketService GET /api/v1/tickets&name={UserName}
- BonusService GET /api/v1/bonuses/{UserName} - 200 {"balance", "status"} бонусы пользователя
- BonusService GET /api/v1/history/{UserName} - 200 история пользователей



# TicketService

- TicketService POST /api/v1/tickets/, тело {билет} - 202
- TicketService GET /api/v1/tickets/{ticketUid}, param: ticketUid - 200, 404
- TicketService DELETE /api/v1/tickets/{ticketUid}, param: ticketUid - 204, 404
- TicketService GET /api/v1/tickets&name={UserName}

# FlightService

- FlightService GET /api/v1/flights - возвращает все рейсы
- FlightService GET /api/v1/flights/{flightNumber} - возвращает 200 если такой рейс существует

# BonusService

- BonusService GET /api/v1/bonuses/{UserName} - 200 {"balance", "status"} бонусы пользователя
- BonusService GET /api/v1/history/{UserName} - 200 история пользователей
- BonusService POST /api/v1/bonuses/reduce, тело {"name", "bonuses"} - 400, 202 {"balance", "status"}, 404
- BonusService POST /api/v1/bonuses/add, тело {"name", "bonuses"} - 400, 202 {"balance", "status"}, 404
- BonusService POST /api/v1/bonuses/calculate_price, тело {"name", "price"} - 400, 202 {"paidByMoney", "paidByBonuses"}
- BonusService POST /api/v1/bonuses/cancel, тело {"name", "ticket"} - 400, 202 {"balance", "status"}, 404