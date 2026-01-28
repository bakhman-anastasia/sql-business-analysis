
/* Проект «Разработка витрины и решение ad-hoc задач»
 * Цель проекта: подготовка витрины данных маркетплейса «ВсёТут»
 * и решение четырех ad hoc задач на её основе
 * 
 * Автор:Бахман Анастасия  
 * Дата: 
*/

/* Часть 1. Разработка витрины данных
 * Напишите ниже запрос для создания витрины данных
*/
WITH filtered_orders AS (
    SELECT
        o.order_id,
        o.buyer_id,
        o.order_status,
        o.order_purchase_ts
    FROM ds_ecom.orders o
    WHERE o.order_status IN ('Доставлено', 'Отменено')
),
top_regions AS (
    SELECT
        u.region
    FROM filtered_orders fo
    JOIN ds_ecom.users u ON u.buyer_id = fo.buyer_id
    GROUP BY u.region
    ORDER BY COUNT(*) DESC
    LIMIT 3
),
order_costs AS (
    SELECT
        oi.order_id,
        SUM(oi.price + oi.delivery_cost) AS order_cost
    FROM ds_ecom.order_items oi
    GROUP BY oi.order_id
),
order_ratings AS (
    SELECT
        r.order_id,
        AVG(r.review_score) AS order_rating,
        COUNT(*)            AS num_reviews
    FROM ds_ecom.order_reviews r
    GROUP BY r.order_id
),
payment_features AS (
    SELECT
        p.order_id,
        MAX(CASE WHEN p.payment_installments > 1 THEN 1 ELSE 0 END) AS used_installments_order,
        MAX(CASE WHEN p.payment_type = 'промокод'        THEN 1 ELSE 0 END) AS has_promo,
        MAX(CASE WHEN p.payment_type = 'денежный перевод' THEN 1 ELSE 0 END) AS used_money_transfer_order
    FROM ds_ecom.order_payments p
    GROUP BY p.order_id
),
orders_enriched AS (
    SELECT
        u.user_id,
        u.region,
        fo.order_id,
        fo.order_status,
        fo.order_purchase_ts,
        oc.order_cost,
        orr.order_rating,
        orr.num_reviews,
        pf.used_installments_order,
        pf.has_promo,
        pf.used_money_transfer_order
    FROM filtered_orders fo
    JOIN ds_ecom.users u
      ON u.buyer_id = fo.buyer_id
    LEFT JOIN order_costs   oc  ON oc.order_id  = fo.order_id
    LEFT JOIN order_ratings orr ON orr.order_id = fo.order_id
    LEFT JOIN payment_features pf ON pf.order_id = fo.order_id
    WHERE u.region IN (SELECT region FROM top_regions)
),
user_region_agg AS (
    SELECT
        user_id,
        region,
        MIN(order_purchase_ts) AS first_order_ts,
        MAX(order_purchase_ts) AS last_order_ts,
        (MAX(order_purchase_ts) - MIN(order_purchase_ts)) AS lifetime,
        COUNT(DISTINCT order_id) AS total_orders,
        AVG(order_rating) AS avg_order_rating,
        COUNT(order_id) FILTER (WHERE order_rating IS NOT NULL) AS num_orders_with_rating,
        COUNT(order_id) FILTER (WHERE order_status = 'Отменено') AS num_canceled_orders,
        COUNT(order_id) FILTER (WHERE order_status = 'Отменено')::numeric
            / COUNT(DISTINCT order_id) AS canceled_orders_ratio,
        SUM(order_cost) AS total_order_costs,
        AVG(order_cost) AS avg_order_cost,
        COUNT(DISTINCT order_id) FILTER (WHERE used_installments_order = 1) AS num_installment_orders,
        COUNT(DISTINCT order_id) FILTER (WHERE has_promo = 1) AS num_orders_with_promo,
        MAX(used_money_transfer_order) AS used_money_transfer,
        MAX(used_installments_order)   AS used_installments,
        MAX(CASE WHEN order_status = 'Отменено' THEN 1 ELSE 0 END) AS used_cancel
    FROM orders_enriched
    GROUP BY user_id, region
)
SELECT *
FROM user_region_agg
ORDER BY lifetime desc


/* Часть 2. Решение ad hoc задач
 * Для каждой задачи напишите отдельный запрос.
 * После каждой задачи оставьте краткий комментарий с выводами по полученным результатам.
*/

/* Задача 1. Сегментация пользователей 
 * Разделите пользователей на группы по количеству совершённых ими заказов.
 * Подсчитайте для каждой группы общее количество пользователей,
 * среднее количество заказов, среднюю стоимость заказа.
 * 
 * Выделите такие сегменты:
 * - 1 заказ — сегмент 1 заказ
 * - от 2 до 5 заказов — сегмент 2-5 заказов
 * - от 6 до 10 заказов — сегмент 6-10 заказов
 * - 11 и более заказов — сегмент 11 и более заказов
*/
-- Напишите ваш запрос тут
with segment as (select user_id, total_order_costs,
       total_orders,
       case
           when total_orders = 1 then 1
           when total_orders between 2 and 5 then 2
           when total_orders between 6 and 10 then 3
           when total_orders >= 11 then 4
       end as segment
from ds_ecom.product_user_features puf)

select segment,
COUNT(user_id) as total_users,
AVG(total_orders) as avg_total_orders,
AVG(total_order_costs) as avg_total_order_costs
from segment
group by segment 
order by segment 

/* Напишите краткий комментарий с выводами по результатам задачи 1.
 * 
*/-- Из полученной таблицы видно, что большинство пользователей (около 60 тыс.) попадает в сегмент с одним заказом.
--Количество пользователей, делающих больше 11 заказов, минимально — в данных только 1 такой пользователь.
--При этом максимальная средняя суммарная стоимость заказов на пользователя наблюдается в сегментах 
--с большим числом заказов (6–10 и 11+ заказов).

/* Задача 2. Ранжирование пользователей 
 * Отсортируйте пользователей, сделавших 3 заказа и более, по убыванию среднего чека покупки.  
 * Выведите 15 пользователей с самым большим средним чеком среди указанной группы.
*/
-- Напишите ваш запрос тут
select user_id,
       total_orders,
       avg_order_cost
 from ds_ecom.product_user_features puf
 where total_orders >= 3
 order by avg_order_cost desc 
 limit 15

/* Напишите краткий комментарий с выводами по результатам задачи 2.
 * 
*/--Среди пользователей, сделавших 3 и более заказов, выделяется небольшая группа из 15 клиентов
 --с самым высоким средним чеком (от ~5,5 до 14,7 тыс. ₽ за заказ). Эти пользователи при
 --относительно небольшом количестве заказов приносят высокую выручку на один заказ, и могут
 --быть хорошей целевой аудиторией для персональных предложений или премиальных сервисов».

/* Задача 3. Статистика по регионам. 
 * Для каждого региона подсчитайте:
 * - общее число клиентов и заказов;
 * - среднюю стоимость одного заказа;
 * - долю заказов, которые были куплены в рассрочку;
 * - долю заказов, которые были куплены с использованием промокодов;
 * - долю пользователей, совершивших отмену заказа хотя бы один раз.
*/

-- Напишите ваш запрос тут
SELECT
    region,
    -- общее число клиентов и заказов
    COUNT(DISTINCT user_id)              AS total_users,
    SUM(total_orders)                    AS total_orders,
    -- средняя стоимость одного заказа (в регионе в целом)
    SUM(total_order_costs)::numeric
        / (SUM(total_orders))   AS avg_order_cost,
    -- доля заказов в рассрочку
    SUM(num_installment_orders)::numeric
        / (SUM(total_orders))   AS share_installment_orders,
    -- доля заказов с промокодом
    SUM(num_orders_with_promo)::numeric
        / (SUM(total_orders))   AS share_promo_orders,
    -- доля пользователей, у которых была хотя бы одна отмена
    SUM(used_cancel)::numeric
        / (COUNT(user_id)) AS share_users_with_cancel
FROM ds_ecom.product_user_features
GROUP BY region
ORDER BY region;

/* Напишите краткий комментарий с выводами по результатам задачи 3.
 * 
*/--В выборке больше всего пользователей и заказов приходится на Москву — их существенно
--больше, чем в Новосибирской области и Санкт-Петербурге.
--При этом средний чек одного заказа выше всего в Новосибирской области, затем идёт Санкт-
--Петербург, а в Москве он самый низкий из трёх регионов.
--Доля заказов в рассрочку и с промокодами выше в регионах (особенно в Санкт-Петербурге), тогда
--как в Москве немного больше доля пользователей, которые хотя бы один раз отменяли заказ.


/* Задача 4. Активность пользователей по первому месяцу заказа в 2023 году
 * Разбейте пользователей на группы в зависимости от того, в какой месяц 2023 года они совершили первый заказ.
 * Для каждой группы посчитайте:
 * - общее количество клиентов, число заказов и среднюю стоимость одного заказа;
 * - средний рейтинг заказа;
 * - долю пользователей, использующих денежные переводы при оплате;
 * - среднюю продолжительность активности пользователя.
*/

-- Напишите ваш запрос тут
WITH users_2023 AS (
    SELECT
        user_id,
        first_order_ts,
        last_order_ts,
        lifetime,
        total_orders,
        avg_order_rating,
        num_orders_with_rating,
        total_order_costs,
        avg_order_cost,
        used_money_transfer
    FROM ds_ecom.product_user_features
    WHERE first_order_ts >= DATE '2023-01-01'
      AND first_order_ts <  DATE '2024-01-01'
)
SELECT
    DATE_TRUNC('month', first_order_ts)::date AS first_order_month,   -- месяц первого заказа
    COUNT(DISTINCT user_id)                          AS total_users,  -- число клиентов
    SUM(total_orders)                                AS total_orders, -- число заказов
    -- средняя стоимость одного заказа (по всем пользователям группы)
    SUM(total_order_costs)::numeric
        / NULLIF(SUM(total_orders), 0)               AS avg_order_cost,
    -- средний рейтинг заказов:
    -- взвешиваем рейтинг пользователя на число его оценённых заказов
    SUM(avg_order_rating * num_orders_with_rating)::numeric
        / (SUM(num_orders_with_rating))     AS avg_order_rating,
    -- доля пользователей, использовавших денежные переводы
    SUM(used_money_transfer)::numeric
        / (COUNT(DISTINCT user_id))         AS share_users_money_transfer,
    -- средняя продолжительность активности пользователя
    AVG(lifetime)                                    AS avg_lifetime
FROM users_2023
GROUP BY DATE_TRUNC('month', first_order_ts)
ORDER BY first_order_month;

/* Напишите краткий комментарий с выводами по результатам задачи 4.
 * --В течение 2023 года число пользователей, сделавших первый заказ, заметно растёт от начала года к
 * --концу, достигая пика в ноябре–декабре. Средний чек одного заказа по группам достаточно
 * --стабильный (примерно 2.5–3.2 тыс. ₽), с небольшим повышением весной и в конце года, а средний
 * --рейтинг удерживается на уровне около 4.1–4.3. Около 20% пользователей в каждой группе используют
 * --денежные переводы при оплате, а более короткий lifetime у «поздних» месячных cohorts
 * --объясняется тем, что у них просто меньше времени наблюдения до конца 2023 года.
		
		
		
	





