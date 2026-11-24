# ДЗ №5 — Online Cinema

Python: Movie Service → Kafka → ClickHouse Kafka Engine → Materialized View → raw_events.
Aggregation Service читает ClickHouse, сохраняет агрегаты в ClickHouse и PostgreSQL, экспортирует PostgreSQL → MinIO.
Grafana читает только материализованные агрегаты ClickHouse.

## Запуск

Из этой папки:

```bash
docker-compose up --build -d
```

Если используется Compose V2, эквивалентная команда: `docker compose up --build -d`.
Топик, версия схемы, таблицы, бакет, datasource и дашборд создаются автоматически. Данные сохраняются в Docker volumes. MinIO собирается автоматически из официального исходного релиза: прежние готовые образы удалены из реестров. Поэтому первая сборка требует загрузки зависимостей и занимает больше времени.

| Компонент | Адрес | Вход |
| --- | --- | --- |
| Общий Swagger обоих сервисов | http://localhost:58000/docs | — |
| Grafana | http://localhost:53000/d/online-cinema | admin / cinema |
| MinIO | http://localhost:59001 | cinema / cinema-secret |
| Schema Registry | http://localhost:58081/subjects/movie-events-value/versions | — |
| ClickHouse HTTP | http://localhost:58123 | cinema / cinema |

Общий Swagger показывает все три метода. Кнопка Execute отправляет события в Movie Service, а пересчёт и экспорт — напрямую в Aggregation Service.

Отправка события через `POST /events`: все восемь полей обязательны. `event_id` — UUID, время — ISO 8601 с часовым поясом, нормализуется в UTC с точностью до миллисекунд. `progress_seconds` — неотрицательное целое, для поиска используется 0. JSON проверяется по типам API и Avro-схеме до публикации. Успешный ответ 201 содержит `event_id`; ошибочные данные дают 422, ошибка публикации — 503.

Пример:

```bash
curl -X POST http://localhost:58000/events -H 'Content-Type: application/json' -d '{"event_id":"b83e4450-f3bc-4a16-8ea5-25e29acfe056","user_id":"user-1","movie_id":"movie-1","event_type":"VIEW_STARTED","timestamp":"2026-11-24T09:00:00Z","device_type":"TV","session_id":"session-1","progress_seconds":0}'
```

Генератор — второй режим того же Movie Service; по умолчанию работает непрерывно. Для трёх сессий:

```bash
docker compose run --rm movie python -m app.movie --generate --sessions 3 --interval 1
```

Последовательность сессии: SEARCHED → VIEW_STARTED → VIEW_PAUSED → VIEW_RESUMED → VIEW_FINISHED → LIKED; прогресс растёт, пользователь, фильм, устройство и сессия внутри последовательности сохраняются. Генератор запускается по команде, при обычном старте синтетические события не создаются.

Пересчёт и экспорт нужной даты:

```bash
curl -X POST 'http://localhost:58002/recalculate?date=2026-11-24'
curl -X POST 'http://localhost:58002/export?date=2026-11-24'
```

Плановый интервал задаётся `AGGREGATION_INTERVAL_SECONDS` (по умолчанию 300 секунд). Каждый цикл пересчитывает дни с событиями, сегодня и вчера: старые когорты обновляются после появления D1/D7 и запоздалых событий. Затем экспортируются метрики за вчера. Неуспешный экспорт повторяется в следующем цикле. Файл: `s3://movie-analytics/daily/YYYY-MM-DD/aggregates.json`; повторная выгрузка перезаписывает тот же объект. Ручной экспорт читает последний успешный результат из PostgreSQL; для обновления данных сначала вызвать пересчёт.

## Проверка

Одна команда, включая сборку и запуск зависимостей:

```bash
docker compose --profile test run --build --rm integration-tests
```

Тесты проверяют схему и репликацию топика, ошибки входных данных, все поля события в ClickHouse, DAU/среднее/конверсию/топ фильмов/retention, повторную доставку события, повторный пересчёт, перезапись экспорта и выполнение запросов Grafana через datasource. Используются уникальные пользователи и свободные исторические даты; по завершении удаляются только собственные тестовые данные. Запуск воспроизводим без ручной подготовки.

## Решения по заданию

- Avro-контракт `schemas/movie-event-v1.avsc` регистрируется под subject `movie-events-value`; на чистом старте это версия 1, при повторном старте идентичная схема повторно не версионируется. Совместимость — BACKWARD. В сообщении передаётся ID зарегистрированной схемы; ClickHouse получает её из Schema Registry.
- Ключ Kafka — `user_id`: события одного пользователя последовательно попадают в одну из трёх партиций, включая разные фильмы. Producers ждут `acks=all`; идемпотентный producer повторяет временно неуспешные запросы с exponential backoff 200–5000 мс в пределах 30 секунд. После исчерпания времени API возвращает ошибку, а не неподтверждённый успех.
- Три брокера одновременно участвуют в KRaft-кворуме: после потери одного остаётся большинство контроллеров. Репликация `movie-events` — 2, `min.insync.replicas=1`; это позволяет продолжить работу на оставшейся копии, но не обещает сохранность при потере обеих копий. Health checks есть у всех постоянно работающих компонентов; `init` и тесты — завершающиеся задания.
- Raw-таблица — ReplacingMergeTree, месячные партиции и сортировка `(день, user_id, event_id)`: дневные выборки и чтение по пользователю. Чтение `FINAL` исключает повторные копии одного неизменного события при повторной доставке. Повторный `event_id` должен обозначать то же событие, а не исправление с другими полями.
- Дневные результаты — `daily_metrics`: дата, имя, измерение (ID фильма либо пустая строка), значение, UTC-время расчёта. В ClickHouse ReplacingMergeTree хранит версии результатов; запросы с `FINAL` читают последнюю. В PostgreSQL составной первичный ключ `(metric_date, metric_name, dimension)` и `ON CONFLICT DO UPDATE` обеспечивают идемпотентность. Вся запись дня в PostgreSQL — одна транзакция, ошибки повторяются с backoff и логируются.
- DAU — уникальные пользователи со всеми типами событий за UTC-день. Среднее время — `avg(progress_seconds)` только для VIEW_FINISHED; без завершений — NULL. Топ фильмов — число VIEW_STARTED по фильму в порядке убывания. Конверсия — количество VIEW_FINISHED / VIEW_STARTED за день, без начатых просмотров — NULL. Это отношение событий, не уникальных сессий; завершение вчерашнего просмотра может дать значение выше 100%.
- Когорта — UTC-дата самого первого VIEW_STARTED пользователя за всю историю. Возвращение — любое событие ровно через N календарных дней; retention — доля вернувшихся от размера когорты. Сохраняются D0–D7, D0=100%; для ещё не наступившего дня или пустой когорты — NULL. Дата строки retention означает дату когорты. Текущий UTC-день может быть неполным.
- Grafana: таблица Retention Cohort Heatmap с цветом ячеек от красного к зелёному, столбцы Day 0–Day 7; ещё две панели — DAU и конверсия. Все запросы используют только `daily_metrics FINAL`.

Реализация ограничена компонентами и функциями задания. Основа из лекций: границы сервисов, HTTP/валидация, Avro и Schema Registry, партиции/репликация/acks, идемпотентность, SQL-транзакции, аналитические агрегаты и продуктовые метрики. Конкретные настройки интеграций сверены с документацией [Kafka](https://kafka.apache.org/39/getting-started/quickstart/), [AvroConfluent в ClickHouse](https://clickhouse.com/docs/interfaces/formats/AvroConfluent), [Schema Registry](https://docs.confluent.io/platform/current/schema-registry/develop/api.html) и [ClickHouse datasource Grafana](https://grafana.com/docs/plugins/grafana-clickhouse-datasource/latest/configure/).
