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
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0755)
	errHandle(err, "创建文件失败")
	defer file.Close()
	write := bufio.NewWriter(file)
	write.WriteString(fmt.Sprintf("-- Host: %s     Database: %s \n", c.Host, c.Schema))
	write.WriteString(fmt.Sprintf("-- Date: %s     Support: %s \n\n\n", time.Now().Format("2006-01-02 15:04:05"), "ysfzf@hotmail.com"))

	tables := GetTables(db, c.Schema)
	checkType := 0 //不检测是否备份数据
	cktables := checkTable{}
	if len(c.Ignores) > 0 {
		checkType = 2
		cktables = arrayToMap(c.Ignores)
	} else {
		if len(c.Tables) > 0 {
			checkType = 1
			cktables = arrayToMap(c.Tables)
		}
	}

	write.WriteString("SET FOREIGN_KEY_CHECKS=0;\n\n")
	for _, tab := range tables {
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
		if checkType == 2 {
			//是否在排除列表中
			_, ok := cktables[tab]
			if ok {
				continue
			}
		} else {
			if checkType == 1 {
				//是否在充许列表中
				_, ok := cktables[tab]
				if !ok {
					continue
				}
			}
		}
		count := GetRowsCount(db, tab)
		if count > 0 {
			// write.WriteString(fmt.Sprintf("LOCK TABLES `%s` WRITE;\n", tab))
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
			// write.WriteString("UNLOCK TABLES;\n\n\n")

			write.WriteString("\n\n\n\n")

		}

	}

	write.Flush()
}

func GetTables(db *sql.DB, dbname string) []string {

	ret := find(db, "show tables from "+dbname)

	result := []string{}
	for _, v := range *ret {
		for _, tab := range v {
			result = append(result, tab)
		}
	}
	return result
}

func GetCreateTable(db *sql.DB, table string) string {
	sql := "show create table " + table
	ret := find(db, sql)
	// fmt.Printf("%+v", ret)
	if len(*ret) > 0 {
		return (*ret)[0]["Create Table"]
	}
	return ""
}

func GetRowsCount(db *sql.DB, table string) int64 {
	sql := "select count(*) as aggregate from " + table

	ret := find(db, sql)
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
	sql := fmt.Sprintf("select * from %s limit %v offset %v", table, limit, offset)
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
		if s == '\'' {
			if ret[len(ret)-1] != '\\' {
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
