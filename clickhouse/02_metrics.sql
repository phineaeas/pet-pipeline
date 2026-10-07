-- =====================================================================
-- Метрики по основной таблице events_agg.
-- Запускать по одному запросу в http://localhost:8123/play
--
-- Правило для events_agg: всегда GROUP BY + функции Merge/sum.
-- Строки с одинаковым ключом могут быть еще не слиты, а так результат
-- правильный в любом случае.
-- =====================================================================


-- 1. DAU: уникальные пользователи за день.
--    uniqMerge объединяет состояния всех категорий, поэтому человек,
--    который смотрел и книги, и спорт, посчитается один раз.
SELECT
    event_date,
    uniqMerge(users) AS dau
FROM pipeline.events_agg
GROUP BY event_date
ORDER BY event_date;


-- 2. Воронка по дням: сколько каких действий и конверсия между шагами.
--    nullIf(x, 0) превращает 0 в NULL, чтобы не делить на ноль.
--
--    ВАЖНО: псевдонимы (AS ...) не должны совпадать с именами колонок.
--    В ClickHouse псевдоним виден во всем запросе: после "sum(carts) AS carts"
--    любое слово carts дальше означает уже sum(carts), и выражение
--    sum(carts) превращается в sum(sum(carts)) - ошибка ILLEGAL_AGGREGATION.
SELECT
    event_date,
    sum(views)     AS total_views,
    sum(carts)     AS total_carts,
    sum(purchases) AS total_purchases,
    round(100 * total_carts / nullIf(total_views, 0), 1)     AS view_to_cart_pct,
    round(100 * total_purchases / nullIf(total_carts, 0), 1) AS cart_to_purchase_pct
FROM pipeline.events_agg
GROUP BY event_date
ORDER BY event_date;


-- 3. Выручка, покупатели и средний чек по категориям за каждый день.
SELECT
    event_date,
    category,
    sum(revenue)        AS total_revenue,
    sum(purchases)      AS total_purchases,
    uniqIfMerge(buyers) AS total_buyers,
    round(toFloat64(total_revenue) / nullIf(total_purchases, 0), 2) AS avg_check
FROM pipeline.events_agg
GROUP BY event_date, category
ORDER BY event_date, total_revenue DESC;


-- 4. Контроль качества: число событий в events_raw должно совпадать
--    с суммой всех действий в events_agg. Если матвьюха что-то потеряла
--    или день задвоился, ok станет 0.
SELECT
    event_date,
    raw_events,
    agg_events,
    raw_events = agg_events AS ok
FROM
    (SELECT event_date, count() AS raw_events
     FROM pipeline.events_raw GROUP BY event_date) AS r
JOIN
    (SELECT event_date, sum(views) + sum(carts) + sum(purchases) AS agg_events
     FROM pipeline.events_agg GROUP BY event_date) AS a
USING (event_date)
ORDER BY event_date;