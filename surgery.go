package main

import (
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/antchfx/xmlquery"
)

func parse_surgery() {

	for dbname := range dbmap {

		var responsesquery string

		responsesquery = `SELECT id, document FROM logging_vimis."vimis_history" vh
			where surgery_parsed_datetime is null
			order by log_date_time desc
			limit 10000
			`

		rows, err := dbmap[dbname].Query(responsesquery)

		if err != nil {
			fmt.Println(dbname, err)
		}

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
									set surgery_parsed_datetime = now(),
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

			fmt.Println("surgery Id:", _respStruct.Id)
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
									set surgery_parsed_datetime = now(),
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

			MainAreas := xmlquery.Find(doc, "//section[code[@code='SUR'][@codeSystem='1.2.643.5.1.13.13.99.2.197']]")
			for _, area := range MainAreas {
				var dateStart, dateEnd string
				var uslNomCode, uslNomName, description *string
				var apparatCode, apparatName, implantCode, implantName *string
				var operClarCode, operClarName, onkoOperCode, onkoOperName *string
				var operDescription, anestesyCode, anestesyName, numProtocol *string
				uslArea := xmlquery.FindOne(area, "//procedure[code[@codeSystem='1.2.643.5.1.13.13.11.1070']]")
				if uslArea != nil {
					uslNomArea := uslArea.SelectElement("code")
					if uslNomArea != nil {
						uslNomCode = RefReturn(uslArea.SelectAttr("code"))
						uslNomName = RefReturn(uslArea.SelectAttr("displayName"))
					}

					descrArea := uslArea.SelectElement("text")
					if descrArea != nil {
						description = RefReturn(descrArea.InnerText())
					}

					dateArea := uslArea.SelectElement("effectiveTime")
					if dateArea != nil {
						lowArea := dateArea.SelectElement("low")
						if lowArea != nil {
							dateStart = lowArea.SelectAttr("value")
							highArea := dateArea.SelectElement("high")
							if highArea != nil {
								dateEnd = highArea.SelectAttr("value")
							}
						} else {
							dateStart = dateArea.SelectAttr("value")
						}
					}

					apparatArea := xmlquery.Find(uslArea, "//code[@codeSystem='1.2.643.5.1.13.13.11.1048']")
					vappcode := ""
					vappname := ""
					for _, area1 := range apparatArea {
						vappcode = vappcode + ";" + area1.SelectAttr("code")
						vappname = vappname + ";" + area1.SelectAttr("displayName")
					}
					vappcode = strings.TrimPrefix(vappcode, ";")
					vappname = strings.TrimPrefix(vappname, ";")

					apparatCode = &vappcode
					apparatName = &vappname

					implArea := xmlquery.Find(uslArea, "//code[@codeSystem='1.2.643.5.1.13.13.11.1079']")
					vimplcode := ""
					vimplname := ""
					for _, area1 := range implArea {
						vimplcode = vimplcode + ";" + area1.SelectAttr("code")
						vimplname = vimplname + ";" + area1.SelectAttr("displayName")
					}
					vimplcode = strings.TrimPrefix(vimplcode, ";")
					vimplname = strings.TrimPrefix(vimplname, ";")

					implantCode = &vimplcode
					implantName = &vimplname

					operArea := xmlquery.Find(uslArea, "//value[@codeSystem='1.2.643.5.1.13.13.99.2.812']")
					vopercode := ""
					vopername := ""
					for _, area1 := range operArea {
						vopercode = vopercode + ";" + area1.SelectAttr("code")
						vopername = vopername + ";" + area1.SelectAttr("displayName")
					}
					vopercode = strings.TrimPrefix(vopercode, ";")
					vopername = strings.TrimPrefix(vopername, ";")

					operClarCode = &vopercode
					operClarName = &vopername

					onkoOperArea := xmlquery.Find(uslArea, "//value[@codeSystem='1.2.643.5.1.13.13.99.2.574']")
					vonkoOpercode := ""
					vonkoOpername := ""
					for _, area1 := range onkoOperArea {
						vonkoOpercode = vonkoOpercode + ";" + area1.SelectAttr("code")
						vonkoOpername = vonkoOpername + ";" + area1.SelectAttr("displayName")
					}
					vonkoOpercode = strings.TrimPrefix(vonkoOpercode, ";")
					vonkoOpername = strings.TrimPrefix(vonkoOpername, ";")

					onkoOperCode = &vonkoOpercode
					onkoOperName = &vonkoOpername

					descrArea = xmlquery.FindOne(uslArea, "//code[@codeSystem='1.2.643.5.1.13.13.99.2.166'][@code='6033']")
					if descrArea != nil {
						operDescription = RefReturn(descrArea.Parent.SelectElement("value").InnerText())
					}

					anestesyArea := xmlquery.Find(uslArea, "//code[@codeSystem='1.2.643.5.1.13.13.11.1033']")
					vanestesyCode := ""
					vanestesyName := ""
					for _, area2 := range anestesyArea {
						vanestesyCode = vanestesyCode + ";" + area2.SelectAttr("code")
						vanestesyName = vanestesyName + ";" + area2.SelectAttr("displayName")
					}
					vanestesyCode = strings.TrimPrefix(vanestesyCode, ";")
					vanestesyName = strings.TrimPrefix(vanestesyName, ";")

					anestesyCode = &vanestesyCode
					anestesyName = &vanestesyName

					protocolArea := xmlquery.FindOne(uslArea, "//code[@codeSystem='1.2.643.5.1.13.13.99.2.166'][@code='11003']")
					if protocolArea != nil {
						numProtocol = RefReturn(protocolArea.Parent.SelectElement("value").InnerText())
					}

					if uslNomCode != nil {
						insert := `insert into parse_vimis_semd.surgery
						(
							date_start,
							date_end,
							usl_nom_code,
							usl_nom_name,
							description,
							apparat_code,
							apparat_name,
							implant_code,
							implant_name,
							operation_clarification_code,
							operation_clarification_name,
							onko_oper_code,
							onko_oper_name,
							oper_description,
							anestesy_code,
							anestesy_name,
							protocol_num,
							rf_vimis_history_id
						) values (
							$1,
							$2,
							$3,
							$4,
							$5,
							$6,
							$7,
							$8,
							$9,
							$10,
							$11,
							$12,
							$13,
							$14,
							$15,
							$16,
							$17,
							$18
						) returning id`

						rows1, err := dbmap[dbname].Query(insert,
							NewNullDate(dateStart),
							NewNullDate(dateEnd),
							uslNomCode,
							uslNomName,
							description,
							apparatCode,
							apparatName,
							implantCode,
							implantName,
							operClarCode,
							operClarName,
							onkoOperCode,
							onkoOperName,
							operDescription,
							anestesyCode,
							anestesyName,
							numProtocol,
							_respStruct.Id,
						)

						if err != nil {
							fmt.Println(err)
						}
						defer rows1.Close()

						var Id string
						for rows1.Next() {
							err = rows1.Scan(&Id)
							if err != nil {
								fmt.Println(err)
							}
						}

						fmt.Println("surgery inserted:", Id)
					}
				}
			}

			updateQueryText := `update logging_vimis.vimis_history set surgery_parsed_datetime = now() where id = $1`

			rows1, err := dbmap[dbname].Query(updateQueryText, _respStruct.Id)
			if err != nil {
				fmt.Println(err)
			}
			fmt.Println("surgery Updated history:", _respStruct.Id)

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
