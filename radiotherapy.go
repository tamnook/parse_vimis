package main

import (
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/antchfx/xmlquery"
)

func parse_radiotherapy() {

	for dbname := range dbmap {

		var responsesquery string

		responsesquery = `SELECT id, document FROM logging_vimis."vimis_history" vh
			where radiotherapy_parsed_datetime is null
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
									set radiotherapy_parsed_datetime = now(),
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

			fmt.Println("Id:", _respStruct.Id)
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
									set radiotherapy_parsed_datetime = now(),
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

			MainArea := xmlquery.FindOne(doc, "//section[code[@code='RADIOTHERAPY']]")
			var dateStart, dateEnd string
			var typeCode, typeName, doctorSnils, doctorLastname, doctorFirstname, doctorPatronymic, doctorPositionCode, doctorPositionName *string
			var kratnost, razOchagDoza, sumOchagDoza, fractionCount, localizationDescription *string
			if MainArea != nil {
				Area := xmlquery.FindOne(MainArea, "//code[@codeSystem='1.2.643.5.1.13.13.99.2.783']")
				if Area != nil {
					typeCode = RefReturn(Area.SelectAttr("code"))
					typeName = RefReturn(Area.SelectAttr("displayName"))

					DateArea := Area.Parent.SelectElement("effectiveTime")
					if DateArea != nil {
						dateStart = DateArea.SelectElement("low").SelectAttr("value")
						if DateArea.SelectElement("high") != nil {
							dateEnd = DateArea.SelectElement("high").SelectAttr("value")
						}
					}
				}

				DocArea := xmlquery.FindOne(MainArea, "//assignedEntity[id[@root='1.2.643.100.3']]")

				if DocArea != nil {
					doctorSnils = RefReturn(xmlquery.FindOne(DocArea, "//id[@root='1.2.643.100.3']").SelectAttr("extension"))
					doctorPositionCode = RefReturn(xmlquery.FindOne(DocArea, "//code[@codeSystem='1.2.643.5.1.13.13.11.1002']").SelectAttr("code"))
					doctorPositionName = RefReturn(xmlquery.FindOne(DocArea, "//code[@codeSystem='1.2.643.5.1.13.13.11.1002']").SelectAttr("displayName"))
					doctorLastname = RefReturn(DocArea.SelectElement("assignedPerson").SelectElement("name").SelectElement("family").InnerText())
					doctorFirstname = RefReturn(DocArea.SelectElement("assignedPerson").SelectElement("name").SelectElement("given").InnerText())
					if DocArea.SelectElement("assignedPerson").SelectElement("name").SelectElement("identity:Patronymic") != nil {
						doctorPatronymic = RefReturn(DocArea.SelectElement("assignedPerson").SelectElement("name").SelectElement("identity:Patronymic").InnerText())
					}
				}

				KratArea := xmlquery.FindOne(MainArea, "//code[@codeSystem='1.2.643.5.1.13.13.99.2.166'][@code='6031']")
				if KratArea != nil {
					kratnost = RefReturn(KratArea.Parent.SelectElement("value").InnerText())
				}

				razOchagDozaArea := xmlquery.FindOne(MainArea, "//code[@codeSystem='1.2.643.5.1.13.13.99.2.166'][@code='6029']")
				if razOchagDozaArea != nil {
					razOchagDoza = RefReturn(razOchagDozaArea.Parent.SelectElement("value").SelectAttr("value"))
				}

				sumOchagDozaArea := xmlquery.FindOne(MainArea, "//code[@codeSystem='1.2.643.5.1.13.13.99.2.166'][@code='6044']")
				if sumOchagDozaArea != nil {
					sumOchagDoza = RefReturn(sumOchagDozaArea.Parent.SelectElement("value").SelectAttr("value"))
				}

				fractionCountArea := xmlquery.FindOne(MainArea, "//code[@codeSystem='1.2.643.5.1.13.13.99.2.166'][@code='6045']")
				if fractionCountArea != nil {
					fractionCount = RefReturn(fractionCountArea.Parent.SelectElement("value").SelectAttr("value"))
				}

				localizationDescriptionArea := xmlquery.FindOne(MainArea, "//code[@codeSystem='1.2.643.5.1.13.13.99.2.166'][@code='6054']")
				if localizationDescriptionArea != nil {
					localizationDescription = RefReturn(localizationDescriptionArea.Parent.SelectElement("value").InnerText())
				}

				insertTherapy := `insert into parse_vimis_semd.radiotherapy 
				(
						type_code,
						type_name,
						date_start,
						date_end,
						doctor_snils,
						doctor_lastname,
						doctor_firstname,
						doctor_patronymic,
						doctor_position_code,
						doctor_position_name,
						kratnost,
						raz_ochag_doza,
						sum_ochag_doza,
						fraction_count,
						localization_description,
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
						$15
				) returning id`

				rows1, err := dbmap[dbname].Query(insertTherapy,
					typeCode,
					typeName,
					NewNullDate(dateStart),
					NewNullDate(dateEnd),
					doctorSnils,
					doctorLastname,
					doctorFirstname,
					doctorPatronymic,
					doctorPositionCode,
					doctorPositionName,
					kratnost,
					razOchagDoza,
					sumOchagDoza,
					fractionCount,
					localizationDescription,
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

				fmt.Println("inserted radio:", Id)

				err = rows1.Close()
				if err != nil {
					fmt.Println(err)
				}

				UslAreas := xmlquery.Find(MainArea, "//act[code[@codeSystem='1.2.643.5.1.13.13.11.1070']]")
				for _, area := range UslAreas {
					var code, name *string
					var date string

					if area.SelectElement("code") != nil {
						code = RefReturn(area.SelectElement("code").SelectAttr("code"))
						name = RefReturn(area.SelectElement("code").SelectAttr("displayName"))
					}
					if area.SelectElement("effectiveTime") != nil {
						date = area.SelectElement("effectiveTime").SelectAttr("value")
					}

					if code != nil {
						insertUsl :=
							`insert into parse_vimis_semd.radiotherapy_usl(
							code,
							name,
							usl_datetime,
							rf_radiotherapy_id
						) values (
							$1,
							$2,
							$3,
							$4
						) returning id`

						rows1, err = dbmap[dbname].Query(insertUsl,
							code,
							name,
							NewNullDate(date),
							Id,
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

			updateQueryText := `update logging_vimis.vimis_history set radiotherapy_parsed_datetime = now() where id = $1`

			rows1, err := dbmap[dbname].Query(updateQueryText, _respStruct.Id)
			if err != nil {
				fmt.Println(err)
			}
			fmt.Println("Updated history:", _respStruct.Id)

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
