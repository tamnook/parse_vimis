package main

import (
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/antchfx/xmlquery"
)

func parse_polt() {

	for dbname := range dbmap {

		var responsesquery string

		responsesquery = `SELECT id, document FROM logging_vimis."vimis_history" vh
			where polt_parsed_datetime is null
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
									set polt_parsed_datetime = now(),
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

			fmt.Println("Polt Id:", _respStruct.Id)
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
									set polt_parsed_datetime = now(),
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

			MainArea := xmlquery.FindOne(doc, "//section[code[@code='POLT']]")
			var dateStart, dateEnd string
			var schemaCode, schemaName, doctorSnils, doctorPosCode, doctorPosName, doctorLastname, doctorFirstname, doctorPatronymic *string
			var lineCode, lineName, cycleCode, cycleName, sostavKursa, specRec *string
			if MainArea != nil {
				schemaArea := xmlquery.FindOne(MainArea, "//code[@codeSystem='1.2.643.5.1.13.13.99.2.647']")
				if schemaArea != nil {
					schemaCode = RefReturn(schemaArea.SelectAttr("code"))
					schemaName = RefReturn(schemaArea.SelectAttr("displayName"))

					dateArea := schemaArea.Parent.SelectElement("effectiveTime")
					if dateArea != nil {
						dateStart = dateArea.SelectElement("low").SelectAttr("value")
						dateEnd = dateArea.SelectElement("high").SelectAttr("value")
					}

					authorArea := schemaArea.Parent.SelectElement("performer")
					if authorArea != nil {
						authorArea = xmlquery.FindOne(authorArea, "//id[@root='1.2.643.100.3']")
						if authorArea != nil {
							doctorSnils = RefReturn(authorArea.SelectAttr("extension"))
							posArea := authorArea.Parent.SelectElement("code")
							if posArea != nil {
								doctorPosCode = RefReturn(posArea.SelectAttr("code"))
								doctorPosName = RefReturn(posArea.SelectAttr("displayName"))
							}

							nameArea := xmlquery.FindOne(authorArea.Parent, "//name")
							if nameArea != nil {
								doctorLastname = RefReturn(nameArea.SelectElement("family").InnerText())
								doctorFirstname = RefReturn(nameArea.SelectElement("given").InnerText())
								patr := nameArea.SelectElement("identity:Patronymic")
								if patr != nil {
									doctorPatronymic = RefReturn(patr.InnerText())
								}
							}
						}
					}
				}

				lineArea := xmlquery.FindOne(MainArea, "//code[@codeSystem='1.2.643.5.1.13.13.99.2.166'][@code='6039']")
				if lineArea != nil {
					lineCode = RefReturn(lineArea.Parent.SelectElement("value").SelectAttr("code"))
					lineName = RefReturn(lineArea.Parent.SelectElement("value").SelectAttr("displayName"))
				}

				cycleArea := xmlquery.FindOne(MainArea, "//code[@codeSystem='1.2.643.5.1.13.13.99.2.166'][@code='6040']")
				if cycleArea != nil {
					cycleCode = RefReturn(cycleArea.Parent.SelectElement("value").SelectAttr("code"))
					cycleName = RefReturn(cycleArea.Parent.SelectElement("value").SelectAttr("displayName"))
				}

				sostavArea := xmlquery.FindOne(MainArea, "//code[@codeSystem='1.2.643.5.1.13.13.99.2.166'][@code='6041']")
				if sostavArea != nil {
					sostavKursa = RefReturn(sostavArea.Parent.SelectElement("value").InnerText())
				}

				specArea := xmlquery.FindOne(MainArea, "//code[@codeSystem='2.16.840.1.113883.5.4'][@code='ASSERTION']")
				if specArea != nil {
					specRec = RefReturn(specArea.Parent.SelectElement("value").InnerText())
				}

				if schemaCode != nil {
					insertTherapy := `insert into parse_vimis_diagn.polt 
				(
					schema_code,
					schema_name,
						date_start,
						date_end,
						doctor_snils,
						doctor_lastname,
						doctor_firstname,
						doctor_patronymic,
						doctor_position_code,
						doctor_position_name,
						line_code,
						line_name,
						cycle_code,
						cycle_name,
						sostav_kursa,
						special_recomendations,
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
						$17
				) returning id`

					rows1, err := dbmap[dbname].Query(insertTherapy,
						schemaCode,
						schemaName,
						NewNullDate(dateStart),
						NewNullDate(dateEnd),
						doctorSnils,
						doctorLastname,
						doctorFirstname,
						doctorPatronymic,
						doctorPosCode,
						doctorPosName,
						lineCode,
						lineName,
						cycleCode,
						cycleName,
						sostavKursa,
						specRec,
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

					fmt.Println("Polt inserted:", Id)

					err = rows1.Close()
					if err != nil {
						fmt.Println(err)
					}

					preps := xmlquery.Find(MainArea, "//code[@codeSystem='1.2.643.5.1.13.13.99.2.611']")
					for _, area := range preps {
						var dateStart, dateEnd string
						var smnnCode, smnnName *string

						smnnCode = RefReturn(area.SelectAttr("code"))
						smnnName = RefReturn(area.SelectAttr("displayName"))

						dateArea := area.Parent.SelectElement("effectiveTime")
						if dateArea != nil {
							dateStart = dateArea.SelectElement("low").SelectAttr("value")
							dateEnd = dateArea.SelectElement("high").SelectAttr("value")
						}

						var period *string
						periodArea := xmlquery.FindOne(area.Parent, "//period")
						if periodArea != nil {
							period = RefReturn(periodArea.SelectAttr("value") + periodArea.SelectAttr("unit"))
						}

						var putVvedCode, putVvedName *string
						putArea := xmlquery.FindOne(area.Parent, "//routeCode")
						if putArea != nil {
							putVvedCode = RefReturn(putArea.SelectAttr("code"))
							putVvedName = RefReturn(putArea.SelectAttr("displayName"))
						}

						var razDoza *string
						dozArea := xmlquery.FindOne(area.Parent, "//doseQuantity")
						if dozArea != nil {
							razDoza = RefReturn(dozArea.SelectAttr("value") + dozArea.SelectAttr("unit"))
						}

						var specRec *string
						specArea := xmlquery.FindOne(area.Parent, "//code[@codeSystem='2.16.840.1.113883.5.4'][@code='ASSERTION']")
						if specArea != nil {
							specRec = RefReturn(specArea.Parent.SelectElement("value").InnerText())
						}

						if smnnCode != nil {
							insPrep := `insert into parse_vimis_semd.polt_preparat(
								date_start,
								date_end,
								smnn_code,
								smnn_name,
								period,
								put_vved_code,
								put_vved_name,
								dose,
								special_recomendations,
								rf_polt_id
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
								$10
							) returning id`

							rows1, err = dbmap[dbname].Query(insPrep,
								NewNullDate(dateStart),
								NewNullDate(dateEnd),
								smnnCode,
								smnnName,
								period,
								putVvedCode,
								putVvedName,
								razDoza,
								specRec,
								Id,
							)
							if err != nil {
								fmt.Println(err)
							}

							var PrepId string
							for rows1.Next() {
								err = rows1.Scan(&PrepId)
								if err != nil {
									fmt.Println(err)
								}
							}
							fmt.Println("inserted prep:", PrepId)

							err = rows1.Close()
							if err != nil {
								fmt.Println(err)
							}
						}
					}
				}
			}

			updateQueryText := `update logging_vimis.vimis_history set polt_parsed_datetime = now() where id = $1`

			rows1, err := dbmap[dbname].Query(updateQueryText, _respStruct.Id)
			if err != nil {
				fmt.Println(err)
			}
			fmt.Println("Polt Updated history:", _respStruct.Id)

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
