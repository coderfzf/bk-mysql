package helper

import (
	"bufio"
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"

	yaml "gopkg.in/yaml.v3"
)

type config struct {
	Host     string   `yaml:"host"`
	Port     int      `yaml:"port"`
	User     string   `yaml:"user"`
	Password string   `yaml:"password"`
	Schema   string   `yaml:"database"`
	Tables   []string `yaml:"tables"`
	Ignores  []string `yaml:"ignores"`
	Outfile  string   `yaml:"outfile"`
	Limit    int      `yaml:"limit"`
}

type checkTable map[string]struct{}

func LoadCofig(file string) *config {
	c := config{}
	yamlFile, err := ioutil.ReadFile(file)
	errHandle(err, "获取配置失败")
	err = yaml.Unmarshal(yamlFile, &c)
	errHandle(err, "获取配置失败")
	return &c
}

func (c *config) Conn() *sql.DB {

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%v)/%s", c.User, c.Password, c.Host, c.Port, c.Schema)
	db, err := sql.Open("mysql", dsn)
	errHandle(err, "连接失败")
	err = db.Ping()
	errHandle(err, "连接失败")
	return db
}

func (c *config) Start() {
	db := c.Conn()
	defer db.Close()
	filename := c.Schema + ".sql"
	if len(c.Outfile) > 0 {
		filename = c.Outfile
	}
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0755)
	errHandle(err, "创建文件失败")
	defer file.Close()
	write := bufio.NewWriter(file)
	write.WriteString(fmt.Sprintf("-- Host: %s     Database: %s \n", c.Host, c.Schema))
	write.WriteString(fmt.Sprintf("-- Date: %s     Support: %s \n\n\n", time.Now().Format("2006-01-02 15:04:05"), "https://github.com/mynameisfzf/bk-mysql"))

	tables := GetTables(db, c.Schema)

	var eTables checkTable
	if len(c.Tables) > 0 {
		eTables = arrayToMap(c.Tables) //要导出的表
	} else {
		eTables = arrayToMap(tables)
	}

	ignores := arrayToMap(c.Ignores) //不导出数据的表

	write.WriteString("SET FOREIGN_KEY_CHECKS=0;\n\n")
	count := len(tables)

	for index, tab := range tables {
		log.Printf("(%v/%v) %s \n", index+1, count, tab)
		_, ok := eTables[tab]
		if !ok {
			continue
		}
		run := true
		offset := 0
		createTable := GetCreateTable(db, tab)
		if len(createTable) < 1 {
			continue
		}
		write.WriteString("-- \n")
		write.WriteString(fmt.Sprintf("-- %s\n", tab))
		write.WriteString("-- \n\n")

		write.WriteString(fmt.Sprintf("DROP TABLE IF EXISTS `%s`;\n", tab))
		write.WriteString(fmt.Sprintf("%s;\n\n\n", createTable))

		_, ok = ignores[tab]
		if ok {
			continue
		}
		count := GetRowsCount(db, tab)
		if count > 0 {
			for run {
				data := GetData(db, tab, c.Limit, offset)
				str := strings.Join(*data, ",")
				if len(str) > 0 {
					offset = offset + c.Limit
					write.WriteString(fmt.Sprintf("INSERT INTO `%s` VALUES %s;\n", tab, str))
				} else {
					run = false
				}

			}
			write.WriteString("\n\n\n\n")

		}

	}

	write.Flush()
}

func GetTables(db *sql.DB, dbname string) []string {

	ret := find(db, fmt.Sprintf("show tables from `%s`", dbname))

	result := []string{}
	for _, v := range *ret {
		for _, tab := range v {
			result = append(result, tab)
		}
	}
	return result
}

func GetCreateTable(db *sql.DB, table string) string {
	ret := find(db, fmt.Sprintf("show create table `%s`", table))
	// fmt.Printf("%+v", ret)
	if len(*ret) > 0 {
		return (*ret)[0]["Create Table"]
	}
	return ""
}

func GetRowsCount(db *sql.DB, table string) int64 {
	ret := find(db, fmt.Sprintf("select count(*) as aggregate from `%s`", table))
	if len(*ret) > 0 {
		tmp, ok := (*ret)[0]["aggregate"]
		if ok {
			count, err := strconv.ParseInt(tmp, 10, 64)
			errHandle(err, "获取数据表总记录数失败")
			return count
		}
	}
	return 0
}

func GetData(db *sql.DB, table string, limit, offset int) *[]string {
	sql := fmt.Sprintf("select * from `%s` limit %v offset %v", table, limit, offset)
	result, err := db.Query(sql)
	errHandle(err, "查询数据发生错误")
	cols, _ := result.Columns()

	len := len(cols)
	cache := make([]interface{}, len)
	types := make([]string, len)
	for i := range cache {
		var a interface{}
		cache[i] = &a
	}

	ts, _ := result.ColumnTypes()
	for i, t := range ts {
		name := t.DatabaseTypeName()
		name = strings.TrimPrefix(name, "UNSIGNED ")
		types[i] = name

	}
	data := []string{}
	for result.Next() {
		_ = result.Scan(cache...)
		item := make([]string, len)
		for i, data := range cache {
			v := *data.(*interface{})

			if v == nil {
				item[i] = "null"
			} else {
				switch (types)[i] {
				case "INT", "TINYINT", "SMALLINT", "MEDIUMINT", "INTEGER", "BIGINT", "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC":
					item[i] = fmt.Sprintf("%s", v)
				default:
					item[i] = "'" + parse(fmt.Sprintf("%s", v)) + "'"
				}

			}

		}
		data = append(data, "("+strings.Join(item, ",")+")")
	}
	return &data
}

//替换字符串中的'
func parse(str string) string {
	ret := []rune{}
	for _, s := range str {
		if s == '\'' || s == '\\' {
			len := len(ret)
			if len > 0 {
				if ret[len-1] != '\\' {
					ret = append(ret, '\\')
				}
			} else {
				ret = append(ret, '\\')
			}
		}
		ret = append(ret, s)
	}

	return string(ret)
}
func errHandle(err error, why string) {
	if err != nil {
		log.Fatalf("【%s】%s", why, err)
	}
}

func find(db *sql.DB, sql string) *[]map[string]string {

	ret := []map[string]string{}
	result, err := db.Query(sql)
	errHandle(err, "查询数据发生错误")
	cols, _ := result.Columns()

	len := len(cols)
	cache := make([]interface{}, len)
	// types := make([]string, len)
	for i := range cache {
		var a interface{}
		cache[i] = &a
	}

	for result.Next() {
		_ = result.Scan(cache...)
		item := map[string]string{}
		for i, data := range cache {
			v := *data.(*interface{})

			if v == nil {
				item[cols[i]] = ""
			} else {
				item[cols[i]] = fmt.Sprintf("%s", v)

			}

		}
		ret = append(ret, item)
	}
	return &ret
}

func arrayToMap(items []string) checkTable {
	var set = make(checkTable)

	for _, item := range items {
		set[item] = struct{}{}
	}

	return set
}
