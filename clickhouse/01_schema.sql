-- =====================================================================
-- Схема ClickHouse 
-- Три объекта:
--   events_raw  - таблица-приемник: сюда Spark пишет сырые события
--   events_agg  - основная таблица: агрегаты по дню и категории
--   events_mv   - материализованное представление: на каждую вставку
--                 в events_raw считает агрегаты и дописывает их в events_agg
--
-- Все команды с IF NOT EXISTS, поэтому файл можно применять повторно.
-- =====================================================================

CREATE DATABASE IF NOT EXISTS pipeline;


-- ---------------------------------------------------------------------
-- 1. Таблица-приемник: одна строка = одно событие
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pipeline.events_raw
(
    event_id    String,
    user_id     UInt32,                  
    product_id  UInt32,
    category    LowCardinality(String),  
    price       Decimal(10, 2),          
    event_type  LowCardinality(String),
    event_ts    DateTime,
    event_date  Date,
    loaded_at   DateTime DEFAULT now()   
)
ENGINE = MergeTree
-- Партиция = один день. Так можно целиком удалить и перезалить день,
-- когда DAG в Airflow перезапускается (идемпотентность).
PARTITION BY event_date
-- Порядок хранения строк на диске. По нему же ClickHouse быстро фильтрует.
ORDER BY (event_date, event_type, user_id);


-- ---------------------------------------------------------------------
-- 2. Основная таблица: агрегаты по (день, категория)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pipeline.events_agg
(
    event_date  Date,
    category    LowCardinality(String),

    -- Уникальных пользователей нельзя просто складывать: один человек мог
    -- смотреть несколько категорий. Поэтому храним не число, а "состояние"
    -- подсчета (AggregateFunction), которое умеет корректно объединяться.
    users       AggregateFunction(uniq, UInt32),
    buyers      AggregateFunction(uniqIf, UInt32, UInt8),

    -- Счетчики и суммы складывать можно, им хватает простого типа.
    views       SimpleAggregateFunction(sum, UInt64),
    carts       SimpleAggregateFunction(sum, UInt64),
    purchases   SimpleAggregateFunction(sum, UInt64),
    revenue     SimpleAggregateFunction(sum, Decimal(38, 2))
)
-- AggregatingMergeTree в фоне сливает строки с одинаковым ключом сортировки
-- в одну, объединяя состояния и суммы.
ENGINE = AggregatingMergeTree
PARTITION BY event_date
ORDER BY (event_date, category);


-- ---------------------------------------------------------------------
-- 3. Материализованное представление
-- ---------------------------------------------------------------------
-- Это ТРИГГЕР: срабатывает на каждый INSERT
-- в events_raw, обрабатывает только вставленные строки и пишет результат
-- в events_agg. Данные, вставленные до создания MV, оно не видит.
CREATE MATERIALIZED VIEW IF NOT EXISTS pipeline.events_mv
TO pipeline.events_agg
AS
SELECT
    event_date,
    category,
    uniqState(user_id)                             AS users,      -- State: сохранить состояние, а не число
    uniqIfState(user_id, event_type = 'purchase')  AS buyers,
    countIf(event_type = 'view')                   AS views,
    countIf(event_type = 'add_to_cart')            AS carts,
    countIf(event_type = 'purchase')               AS purchases,
    sumIf(price, event_type = 'purchase')          AS revenue
FROM pipeline.events_raw
GROUP BY event_date, category;
