# Meower user outbox

Сервис ответственный за отправку событий из сервиса пользователей в брокер сообщений. Вместе с [сервисом пользователей](https://github.com/Karzoug/meower-user-service) реализует паттерн transactional outbox. 

Отправка в брокер сообщений осуществляется с гарантией at least once. При необходимости сервис может запускаться в несколько инстансов.

Формат сообщений отправляемых в брокер описан в [репозитории api](https://github.com/Karzoug/meower-api/tree/main/proto/user) (protobuf).