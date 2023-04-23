# Backup MySql

这是一个命令行工具，用来将MySql数据库导出为一个.sql文件。需要在配置文件中配置相关参数：

```yaml
host: localhost
port: 3306
user: root
password: 123456
database: test
ignores:  # 不导出数据的表，但是会导出结构
  - admin_log
outfile: test.sql # 输出文件
limit: 500  # 每条insert语句中的最大记录数

```

# 开始

```
For example:

    backup -f xx.yml

Usage:
  backup [flags]

Flags:
  -f, --config string   config file (default is ./.backup.yaml) (default "./.backup.yaml")
  -h, --help            help for backup

```