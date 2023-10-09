package main

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"strings"
	"time"

	"github.com/lib/pq"
)

func readconfig() {
	data, err := os.Open("conf.json")
	if err != nil {
		fmt.Println("open config file", strings.Split(err.Error(), "\n")[0])
	}

	bytedata, err := ioutil.ReadAll(data)
	if err != nil {
		fmt.Println("open config file", strings.Split(err.Error(), "\n")[0])
	}

	err = json.Unmarshal(bytedata, &config)
	if err != nil {
		fmt.Println("open config file", strings.Split(err.Error(), "\n")[0])
	}

}

func contains(s []string, str string) bool {
	for _, v := range s {
		if v == str {
			return true
		}
	}

	return false
}

type configstruct struct {
	ConnStr      string `json:"conn_string"`
	ConnStrVimis string `json:"conn_string_vimis"`
}

type respStruct struct {
	Id           string `json:"Id"`
	Document     string `json:"Document"`
	Vmcl         string `json:"Vmcl"`
	TriggerPoint string `json:"TriggerPoint"`
}

type uslTestStruct struct {
	UslTestId   string `json:"UslTestId"`
	Description string `json:"Description"`
	LisTestCode string `json:"LisTestCode"`
	Unit        string `json:"Unit"`
}

func DerefString(s *string) string {
	if s != nil {
		return *s
	}
	return ""
}

func NewNullString1(s string) sql.NullString {

	s1 := s

	if len(s1) == 0 {
		return sql.NullString{}
	}
	return sql.NullString{
		String: s1,
		Valid:  true,
	}
}

func RefReturn(s string) *string {
	if s == "" {
		return nil
	} else {
		return &s
	}
}

func NewNullDate(s string) *string {

	var s1 time.Time

	s1, err := time.Parse("02.01.2006 15:04:05", s)
	if err != nil {
		if len(s) > 12 {
			s = s[:12]
		}
		s1, err = time.Parse("200601021504", s)
		var str string

		if err != nil {
			fmt.Println(err)
			s1, err = time.Parse("20060102", s)
			if err != nil {
				fmt.Println(err)
				s1, err = time.Parse("02.01.2006 15:04:05", s)
				if err != nil {
					fmt.Println(err)
					return nil
				} else {
					str = s1.Format("2006-01-02 15:04:05")
					return &str
				}
			} else {
				str = s1.Format("2006-01-02 15:04:05")
				return &str
			}
		} else {
			str = s1.Format("2006-01-02 15:04:05")
			return &str
		}

	} else {
		str := s1.Format("2006-01-02 15:04:05")
		return &str
	}
}

func (self *ViaSSHDialer) Open(s string) (_ driver.Conn, err error) {
	return pq.DialOpen(self, s)
}

func (self *ViaSSHDialer) Dial(network, address string) (net.Conn, error) {
	return self.client.Dial(network, address)
}

func (self *ViaSSHDialer) DialTimeout(network, address string, timeout time.Duration) (net.Conn, error) {
	return self.client.Dial(network, address)
}

func AddDay(s string) string {
	if strings.Contains(s, "д") || strings.Contains(s, "н") {
		return s
	} else {
		return s + " дней"
	}
}

func AddWeek(s string) string {
	if strings.Contains(s, "д") || strings.Contains(s, "н") {
		return s
	} else {
		return s + " недель"
	}
}

func DeleteDoubleSpace(s string) string {
	var ans string

	for _, v := range strings.Split(s, " ") {
		if v != "" {
			ans = ans + " " + v
		}
	}

	ans = strings.TrimPrefix(ans, " ")

	return ans
}
