-- очищаем временную таблицу в dwh
truncate table orders_tmp;
------------------------------------------------
/* вставляем данные во временную таблицу 
из внешней таблицы и считаем хэш по строке */
insert into orders_tmp
select
       id as order_id,
       student_id,
       teacher_id,
       stage,
       status,
       hashtext(ex::text) as row_hash,
       created_at,
       updated_at
from orders_ex ex;
------------------------------------------------
/* обновляем в основной таблице только те строки,
которые совпадают по id заказа, но различаются по 
значению хэша */
update raw_order dwh
set order_id   = tmp.order_id,
    student_id = tmp.student_id,
    teacher_id = tmp.teacher_id,
    stage      = tmp.stage,
    status     = tmp.status,
    row_hash   = tmp.row_hash,
    created_at = tmp.created_at,
    updated_at = tmp.updated_at
from orders_tmp tmp
where dwh.order_id = tmp.order_id
  and dwh.row_hash <> tmp.row_hash;
------------------------------------------------
/* добавляем из временной таблицы строки заказов,
id которых не найден в основной таблице */
insert into raw_order (
    order_id,
    student_id,
    teacher_id,
    stage,
    status,
    row_hash,
    created_at,
    updated_at
)
select
    tmp.order_id,
    tmp.student_id,
    tmp.teacher_id,
    tmp.stage,
    tmp.status,
    tmp.row_hash ,
    tmp.created_at ,
    tmp.updated_at
from orders_tmp tmp left join raw_order dwh
on tmp.order_id = dwh.order_id
where dwh.order_id is null;