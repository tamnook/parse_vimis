package main

import (
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/antchfx/xmlquery"
)

func parse_pmo() {

	for dbname := range dbmap {

		var responsesquery string

		responsesquery = `SELECT id, document FROM logging_vimis."vimis_history" vh
			where pmo_parsed_datetime is null and doc_type = '24'
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
									set pmo_parsed_datetime = now(),
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

			fmt.Println("Pmo Id:", _respStruct.Id)
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
									set pmo_parsed_datetime = now(),
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

			MainArea := xmlquery.FindOne(doc, "//encompassingEncounter[code[@codeSystem='1.2.643.5.1.13.13.99.2.723']]")
			var dateStart, dateEnd string
			if MainArea != nil {
				DateArea := MainArea.SelectElement("effectiveTime")
				dateStart = DateArea.SelectElement("low").SelectAttr("value")
				if DateArea.SelectElement("high") != nil {
					dateEnd = DateArea.SelectElement("high").SelectAttr("value")
				}
			}

			MainArea = xmlquery.FindOne(doc, "//section[code[@code='DOCINFO']]")
			var fullPmo, HealthGroupCode, HealthGroupName *string
			if MainArea != nil {

				fullPmoArea := xmlquery.FindOne(MainArea, "//code[@codeSystem='1.2.643.5.1.13.13.99.2.166'][@code='12100']")
				if fullPmoArea != nil {
					fullPmo = RefReturn(fullPmoArea.Parent.SelectElement("value").SelectAttr("value"))
				}

				HealthGroupArea := xmlquery.FindOne(MainArea, "//code[@codeSystem='1.2.643.5.1.13.13.99.2.166'][@code='12101']")
				if HealthGroupArea != nil {
					HealthGroupCode = RefReturn(HealthGroupArea.Parent.SelectElement("value").SelectAttr("code"))
					HealthGroupName = RefReturn(HealthGroupArea.Parent.SelectElement("value").SelectAttr("displayName"))
				}

			}

			insertTherapy := `insert into parse_vimis_semd.pmo 
				(
						date_start,
						date_end,
						health_group_code,
						health_group_name,
						full_pmo,
						rf_vimis_history_id
				) values (
						$1,
						$2,
						$3,
						$4,
						$5,
						$6
				) returning id`

			rows1, err := dbmap[dbname].Query(insertTherapy,

				NewNullDate(dateStart),
				NewNullDate(dateEnd),
				HealthGroupCode,
				HealthGroupName,
				fullPmo,
				_respStruct.Id,
			)
			if err != nil {
				fmt.Println(err)
			}

			var Id string
			for rows1.Next() {
				err = rows1.Scan(&Id)
				if err != nil {
					fmt.Println(err)
				}
			}

			fmt.Println("Pmo inserted :", Id)

			err = rows1.Close()
			if err != nil {
				fmt.Println(err)
			}

			MainArea = xmlquery.FindOne(doc, "//section[code[@code='MEDEXAMINFO']]")
			if MainArea != nil {

				UslAreas := xmlquery.Find(MainArea, "//observation[code[@codeSystem='1.2.643.5.1.13.13.99.2.822']]")
				for _, area := range UslAreas {
					var activityCode, activityName, status, patologyDetected, UslNomCode, UslNomName *string
					var date *string

					if area.SelectElement("code") != nil {
						activityCode = RefReturn(area.SelectElement("code").SelectAttr("code"))
						activityName = RefReturn(area.SelectElement("code").SelectAttr("displayName"))
					}
					if area.SelectElement("value") != nil {
						status = RefReturn(area.SelectElement("value").SelectAttr("displayName"))
					}

					dateArea := xmlquery.FindOne(area, "//code[@codeSystem='1.2.643.5.1.13.13.99.2.166'][@code='4068']")
					if dateArea != nil {
						date = NewNullDate(dateArea.Parent.SelectElement("effectiveTime").SelectAttr("value"))
					}

					patologyArea := xmlquery.FindOne(area, "//code[@codeSystem='1.2.643.5.1.13.13.99.2.166'][@code='808']")
					if patologyArea != nil {
						patologyDetected = RefReturn(patologyArea.Parent.SelectElement("value").SelectAttr("value"))
					}

					UslNomArea := xmlquery.FindOne(area, "//code[@codeSystem='1.2.643.5.1.13.13.11.1070']")
					if UslNomArea != nil {
						UslNomCode = RefReturn(UslNomArea.SelectAttr("code"))
						UslNomName = RefReturn(UslNomArea.SelectAttr("displayName"))
					}

					if activityCode != nil {
						insertUsl :=
							`insert into parse_vimis_semd.pmo_usl(
							code,
							name,
							datetime,
							rf_pmo_id,
							usl_nom_code,
							usl_nom_name,
							status,
							patology
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

						rows1, err = dbmap[dbname].Query(insertUsl,
							activityCode,
							activityName,
							date,
							Id,
							UslNomCode,
							UslNomName,
							status,
							patologyDetected,
						)
						if err != nil {
							fmt.Println(err)
						}

						var UslId string
						for rows1.Next() {
							err = rows1.Scan(&UslId)
							if err != nil {
								fmt.Println(err)
							}
						}
						fmt.Println("inserted usl:", UslId)

						err = rows1.Close()
						if err != nil {
							fmt.Println(err)
						}
					}
				}
			}

			updateQueryText := `update logging_vimis.vimis_history set pmo_parsed_datetime = now() where id = $1`

			rows1, err = dbmap[dbname].Query(updateQueryText, _respStruct.Id)
			if err != nil {
				fmt.Println(err)
			}
			fmt.Println("Pmo Updated history:", _respStruct.Id)

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
