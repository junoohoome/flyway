create table If not exists BOOK
(
    ID            bigint not null comment '主建',
    NAME          varchar(100) comment '名称',
    PRICE         varchar(100) comment '价格',
    REMARK        varchar(500) comment '描述',
    primary key (ID)
);

alter table BOOK comment '书籍表';

