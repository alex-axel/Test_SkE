-- последовательность для генерации id raw_order
CREATE SEQUENCE raw_order_id START 1;

-- основная таблица с заказами
CREATE TABLE raw_order
(
    id         bigint primary key default nextval('raw_order_id'),
    order_id   bigint,
    student_id bigint,
    teacher_id bigint,
    stage      varchar(10),
    status     varchar(512),
    row_hash   bigint,
    created_at timestamp,
    updated_at timestamp
);

/* промежуточная таблица заказов для вычисления хэша строки 
и обновления основной таблицы */
CREATE TABLE orders_tmp (
    order_id bigint,
    student_id bigint,
    teacher_id bigint,
    stage varchar(10),
    status varchar(512),
    row_hash bigint,
    created_at timestamp,
    updated_at timestamp
);

/* внешняя таблица, предоставляющая доступ к csv-файлу на сервере с помощью
утилиты gpfdist (входит в поставку greenplum); внешние таблицы можно создавать
и для облачных решений по ссылке, поэтому метод будет работать с s3, gcs и тп.*/
CREATE READABLE EXTERNAL TABLE orders_ex (
    id bigint,
    student_id bigint,
    teacher_id bigint,
    stage varchar(10),
    status varchar(512),
    created_at timestamp,
    updated_at timestamp
)
LOCATION ('gpfdist://10.0.0.71:8081//orders.csv')
FORMAT 'CSV' (HEADER);