# Getting Started

### Reference Documentation

For further reference, please consider the following sections:

* 主要对 flyway-community-db-support 模块做了对达梦和人大金创数据库的支持，并删除其他的示例
* 增加 demo 模块示例使用
* 对 flyway-mysql 模块修改支持 MySQL 版本从 8.0 向下兼容到 5.7 开始

### Guides

对于 flyway-community-db-support 如何开发支持新的数据，请参考下面两种任意一种方式:

* 简单的做法，达梦数据库类似于 Oracle ，可以在 flyway-core 模块的路径 `org.flywaydb.core.internal.database` 找出 Oracle 的文件进行拷贝，再调试修改特殊的 sql 语句

* [Contributing Database Compatibility to Flyway](https://flywaydb.org/documentation/contribute/contributingDatabaseSupport#lets-code)

