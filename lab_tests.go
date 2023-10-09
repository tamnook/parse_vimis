package main

import (
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	"github.com/antchfx/xmlquery"
)

func parse_labtest() {

	for dbname := range dbmap {

		var responsesquery string

		responsesquery = `SELECT id, document FROM logging_vimis."vimis_history" vh
			where lab_test_parsed_datetime is null and doc_type in ('3', '8')
			order by log_date_time desc
			limit 10000
			`
		start := time.Now()
		rows, err := dbmap[dbname].Query(responsesquery)

		if err != nil {
			fmt.Println(dbname, err)
		}

		duration := time.Since(start)
		fmt.Println("Select duration:", duration)

		var _respStruct respStruct
		i := 0

		for rows.Next() {
			i++
			if err = rows.Scan(
				&_respStruct.Id,
				&_respStruct.Document,
			); err != nil {
				fmt.Println(dbname, err)
			}

			if _respStruct.Document == "" {
				rows1, err := dbmap[dbname].Query(
					`update logging_vimis.vimis_history 
									set lab_test_parsed_datetime = now(),
									parse_errors = concat(parse_errors, ';', $2::varchar) 
									where id = $1`, _respStruct.Id, "Empty")
				if err != nil {
					fmt.Println(err)
				}

				err = rows1.Close()
				if err != nil {
					fmt.Println(err)
				}
				continue
			}

			fmt.Println("lab_test Id:", _respStruct.Id)
			var bytesdoc, err = base64.StdEncoding.DecodeString(_respStruct.Document)

			if err != nil {
				fmt.Println(err)
			}

			doc, err := xmlquery.Parse(strings.NewReader(strings.ReplaceAll(string(bytesdoc), "1.2.643.5.1.13.13.11.1380", "1.2.643.5.1.13.13.99.2.166")))

			if err != nil {
				fmt.Println(err)
				errstr := err.Error()
				rows1, err := dbmap[dbname].Query(
					`update logging_vimis.vimis_history 
									set lab_test_parsed_datetime = now(),
									parse_errors = concat(parse_errors, ';', $2::varchar) 
									where id = $1`, _respStruct.Id, errstr)
				if err != nil {
					fmt.Println(err)
				}

				err = rows1.Close()
				if err != nil {
					fmt.Println(err)
				}
				continue
			}

			MainArea := xmlquery.FindOne(doc, "//code[@code='RESLAB']")

			if MainArea != nil {

				testAreas := xmlquery.Find(MainArea.Parent, "//code[@codeSystem='1.2.643.5.1.13.13.11.1080']")
				for _, area := range testAreas {
					var code, name *string
					code = RefReturn(area.SelectAttr("code"))
					name = RefReturn(area.SelectAttr("displayName"))

					var date string
					dateArea := area.Parent.SelectElement("effectiveTime")
					if dateArea != nil {
						date = dateArea.InnerText()
						if date == "" {
							date = dateArea.SelectAttr("value")
						}
					}

					var value, unit *string
					resultArea := area.Parent.SelectElement("value")
					if resultArea != nil {
						value = RefReturn(resultArea.InnerText())
						if strings.ReplaceAll(resultArea.InnerText(), " ", "") == "" {
							value = RefReturn(resultArea.SelectAttr("value"))
							unit = RefReturn(resultArea.SelectAttr("unit"))

							translArea := resultArea.SelectElement("translation")
							if translArea != nil {
								value = RefReturn(translArea.SelectAttr("value"))
								unit = RefReturn(translArea.SelectAttr("displayName"))
							}
						}
					}

					var interpretation *string
					interpArea := area.Parent.SelectElement("interpretationCode")
					if interpArea != nil {
						interpretation = RefReturn(interpArea.SelectAttr("code"))
					}

					var ref *string
					refArea := area.Parent.SelectElement("referenceRange")
					if refArea != nil {
						refArea := xmlquery.FindOne(refArea, "//text")
						if refArea != nil {
							if !strings.Contains(refArea.InnerText(), "Описание") {

								ref = RefReturn(DeleteDoubleSpace(refArea.InnerText()))
							}
						}
					}

					if DerefString(code) != "" {

						checksel := `
							select count(*) from parse_vimis_semd.lab_tests 
							where rf_vimis_history_id = $1 and code = $2`

						rows1, err := dbmap[dbname].Query(checksel,
							_respStruct.Id,
							code,
						)
						if err != nil {
							fmt.Println(err)
						}

						var count int
						for rows1.Next() {
							err = rows1.Scan(&count)
							if err != nil {
								fmt.Println(err)
							}
						}

						fmt.Println("lab_test check:", count)

						err = rows1.Close()
						if err != nil {
							fmt.Println(err)
						}

						if count == 0 {

							insertTherapy := `insert into parse_vimis_semd.lab_tests 
						(
							code,
							name,
							datetime,
							result_value,
							unit,
							reference_range,
							result_interpretation,
							rf_vimis_history_id
						) values (
								$1,
								$2,
								$3,
								$4,
								$5,
								$6,
								$7,
								$8
		
						) returning id`
							start1 := time.Now()
							rows1, err := dbmap[dbname].Query(insertTherapy,
								code,
								name,
								NewNullDate(date),
								value,
								unit,
								ref,
								interpretation,
								_respStruct.Id,
							)
							if err != nil {
								fmt.Println(err)
							}

							duration1 := time.Since(start1)
							fmt.Println("insert duration:", duration1)

							var Id string
							for rows1.Next() {
								err = rows1.Scan(&Id)
								if err != nil {
									fmt.Println(err)
								}
							}

							fmt.Println("lab_test inserted :", Id)

							err = rows1.Close()
							if err != nil {
								fmt.Println(err)
							}
						}
					}
				}
			}

			//var  *string

			updateQueryText := `update logging_vimis.vimis_history set lab_test_parsed_datetime = now() where id = $1`

			rows1, err := dbmap[dbname].Query(updateQueryText, _respStruct.Id)
			if err != nil {
				fmt.Println(err)
			}
			fmt.Println("lab_test Updated history:", _respStruct.Id)

			err = rows1.Close()
			if err != nil {
				fmt.Println(err)
			}
		}
		err = rows.Close()

		if err != nil {
			fmt.Println(err)
		}
	}
}
