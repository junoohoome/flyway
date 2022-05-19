
create table BOOK
(
    ID     INT8       not null,
    NAME   VARCHAR(32)  not null,
    PRICE  VARCHAR(64)  not null,
    REMARK VARCHAR(1000) not null,
    constraint PK_BOOK primary key (ID)
);

comment
on table BOOK is
'书籍表';

comment
on column BOOK.ID is
'主建';

comment
on column BOOK.NAME is
'名称';

comment
on column BOOK.PRICE is
'价格';

comment
on column BOOK.REMARK is
'描述';
