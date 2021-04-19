CREATE TABLE orders
(
    id         bigint primary key,
    student_id bigint,
    teacher_id bigint,
    stage      varchar(10),
    status     varchar(512),
    created_at timestamp,
    updated_at timestamp
);