package main

import (
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/antchfx/xmlquery"
)

func parse_newborn() {

	for dbname := range dbmap {

		var responsesquery string

		responsesquery = `SELECT id, document FROM logging_vimis."vimis_history" vh
			where newborn_parsed_datetime is null and doc_type in ('32', '17', '20', '5')
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
									set newborn_parsed_datetime = now(),
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

			fmt.Println("newborn Id:", _respStruct.Id)
			var bytesdoc, err = base64.StdEncoding.DecodeString(_respStruct.Document)

			if err != nil {
				fmt.Println(err)
			}

			doc, err := xmlquery.Parse(strings.NewReader(string(bytesdoc)))

			if err != nil {
				fmt.Println(err)
				errstr := err.Error()
				rows1, err := dbmap[dbname].Query(
					`update logging_vimis.vimis_history 
									set newborn_parsed_datetime = now(),
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

			MainArea := xmlquery.FindOne(doc, "//serviceEvent[code[@codeSystem='1.2.643.5.1.13.13.99.2.726']]")
			var dateStart, dateEnd string
			if MainArea != nil {
				DateArea := MainArea.SelectElement("effectiveTime")
				dateStart = DateArea.SelectElement("low").SelectAttr("value")
				if DateArea.SelectElement("high") != nil {
					dateEnd = DateArea.SelectElement("high").SelectAttr("value")
				}
			}

			MainAreas := xmlquery.Find(doc, "//section[code[@code='NBINFO']]")
			for _, area := range MainAreas {

				var lastname, patronymic, gendercode, gendername, ishodcode, ishodename, plodnumcode, plodnumname, donoshencode, donoshenname *string
				var mestorozhcode, mestorozhname, sposobrodorazrcode, sposobrodorazrname *string
				var birthdate string

				namearea := xmlquery.FindOne(area, "//relatedSubject[code[@codeSystem='1.2.643.5.1.13.13.11.1021']]")
				if namearea != nil {
					namearea = namearea.SelectElement("subject").SelectElement("name")
					if namearea != nil {
						if namearea.SelectElement("family") != nil {
							lastname = RefReturn(namearea.SelectElement("family").InnerText())
						}
						if namearea.SelectElement("identity:Patronymic") != nil {
							patronymic = RefReturn(namearea.SelectElement("identity:Patronymic").InnerText())
						}
					
						if namearea.Parent.SelectElement("administrativeGenderCode") != nil {
							gendercode = RefReturn(namearea.Parent.SelectElement("administrativeGenderCode").SelectAttr("code"))
							gendername = RefReturn(namearea.Parent.SelectElement("administrativeGenderCode").SelectAttr("displayName"))
						}

						if namearea.Parent.SelectElement("birthTime") != nil {
							birthdate = namearea.Parent.SelectElement("birthTime").SelectAttr("value")
						}
					}
				

					ishodarea := xmlquery.FindOne(area, "//value[@codeSystem='1.2.643.5.1.13.13.99.2.279']")
					if ishodarea != nil {
						ishodcode = RefReturn(ishodarea.SelectAttr("code"))
						ishodename = RefReturn(ishodarea.SelectAttr("displayName"))
					} else {
						ishodarea = xmlquery.FindOne(area, "//code[@codeSystem='1.2.643.5.1.13.13.99.2.279']")
						if ishodarea != nil {
							ishodcode = RefReturn(ishodarea.SelectAttr("code"))
							ishodename = RefReturn(ishodarea.SelectAttr("displayName"))
						}
					}

					plodnumarea := xmlquery.FindOne(area, "//value[@codeSystem='1.2.643.5.1.13.13.99.2.493']")
					if plodnumarea != nil {
						plodnumcode = RefReturn(plodnumarea.SelectAttr("code"))
						plodnumname = RefReturn(plodnumarea.SelectAttr("displayName"))
					} else {
						plodnumarea = xmlquery.FindOne(area, "//code[@codeSystem='1.2.643.5.1.13.13.99.2.493']")
						if plodnumarea != nil {
							plodnumcode = RefReturn(plodnumarea.SelectAttr("code"))
							plodnumname = RefReturn(plodnumarea.SelectAttr("displayName"))
						}
					}

					mestorozharea := xmlquery.FindOne(area, "//value[@codeSystem='1.2.643.5.1.13.13.99.2.30']")
					if mestorozharea != nil {
						mestorozhcode = RefReturn(mestorozharea.SelectAttr("code"))
						mestorozhname = RefReturn(mestorozharea.SelectAttr("displayName"))
					} else {
						mestorozharea = xmlquery.FindOne(area, "//code[@codeSystem='1.2.643.5.1.13.13.99.2.30']")
						if mestorozharea != nil {
							mestorozhcode = RefReturn(mestorozharea.SelectAttr("code"))
							mestorozhname = RefReturn(mestorozharea.SelectAttr("displayName"))
						}
					}

					donoshenarea := xmlquery.FindOne(area, "//value[@codeSystem='1.2.643.5.1.13.13.99.2.459']")
					if donoshenarea != nil {
						donoshencode = RefReturn(donoshenarea.SelectAttr("code"))
						donoshenname = RefReturn(donoshenarea.SelectAttr("displayName"))
					} else {
						donoshenarea = xmlquery.FindOne(area, "//code[@codeSystem='1.2.643.5.1.13.13.99.2.459']")
						if donoshenarea != nil {
							donoshencode = RefReturn(donoshenarea.SelectAttr("code"))
							donoshenname = RefReturn(donoshenarea.SelectAttr("displayName"))
						}
					}

					sposobrodorazr := xmlquery.FindOne(area, "//value[@codeSystem='1.2.643.5.1.13.13.99.2.437']")
					if sposobrodorazr != nil {
						sposobrodorazrcode = RefReturn(sposobrodorazr.SelectAttr("code"))
						sposobrodorazrname = RefReturn(sposobrodorazr.SelectAttr("displayName"))
					} else {
						sposobrodorazr = xmlquery.FindOne(area, "//code[@codeSystem='1.2.643.5.1.13.13.99.2.437']")
						if sposobrodorazr != nil {
							sposobrodorazrcode = RefReturn(sposobrodorazr.SelectAttr("code"))
							sposobrodorazrname = RefReturn(sposobrodorazr.SelectAttr("displayName"))
						}
					}

					var massa, dlina, okrpl, okrgol, groupcode, groupname, rezuscode, rezusname *string

					massaarea := xmlquery.FindOne(area, "//observation[code[@codeSystem='1.2.643.5.1.13.13.99.2.262'][@code='50']]")
					if massaarea != nil {
						massa = RefReturn(massaarea.SelectElement("value").SelectAttr("value"))
					} else {
						massaarea = xmlquery.FindOne(area, "//observation[code[@codeSystem='1.2.643.5.1.13.13.11.1010'][@code='50']]")
						if massaarea != nil {
							massa = RefReturn(massaarea.SelectElement("value").SelectAttr("value"))
						}
					}

					dlinaarea := xmlquery.FindOne(area, "//observation[code[@codeSystem='1.2.643.5.1.13.13.99.2.262'][@code='51']]")
					if dlinaarea != nil {
						dlina = RefReturn(dlinaarea.SelectElement("value").SelectAttr("value"))
					} else {
						dlinaarea = xmlquery.FindOne(area, "//observation[code[@codeSystem='1.2.643.5.1.13.13.11.1010'][@code='51']]")
						if dlinaarea != nil {
							dlina = RefReturn(dlinaarea.SelectElement("value").SelectAttr("value"))
						}
					}

					okrplarea := xmlquery.FindOne(area, "//observation[code[@codeSystem='1.2.643.5.1.13.13.99.2.262'][@code='53']]")
					if okrplarea != nil {
						okrpl = RefReturn(okrplarea.SelectElement("value").SelectAttr("value"))
					} else {
						okrplarea := xmlquery.FindOne(area, "//observation[code[@codeSystem='1.2.643.5.1.13.13.11.1010'][@code='53']]")
						if okrplarea != nil {
							okrpl = RefReturn(okrplarea.SelectElement("value").SelectAttr("value"))
						}
					}

					okrgolarea := xmlquery.FindOne(area, "//observation[code[@codeSystem='1.2.643.5.1.13.13.99.2.262'][@code='52']]")
					if okrgolarea != nil {
						okrgol = RefReturn(okrgolarea.SelectElement("value").SelectAttr("value"))
					} else {
						okrgolarea := xmlquery.FindOne(area, "//observation[code[@codeSystem='1.2.643.5.1.13.13.11.1010'][@code='52']]")
						if okrgolarea != nil {
							okrgol = RefReturn(okrgolarea.SelectElement("value").SelectAttr("value"))
						}
					}

					grouparea := xmlquery.FindOne(area, "//entry[observation[code[@codeSystem='1.2.643.5.1.13.13.11.1061']]]")
					if grouparea != nil {
						groupcode = RefReturn(grouparea.SelectElement("observation").SelectElement("code").SelectAttr("code"))
						groupname = RefReturn(grouparea.SelectElement("observation").SelectElement("code").SelectAttr("displayName"))
						rezusarea := xmlquery.FindOne(area, "//entryRelationship[observation[code[@codeSystem='1.2.643.5.1.13.13.11.1061']]]")
						if rezusarea != nil {
							rezuscode = RefReturn(grouparea.SelectElement("observation").SelectElement("code").SelectAttr("code"))
							rezuscode = RefReturn(grouparea.SelectElement("observation").SelectElement("code").SelectAttr("displayName"))
						}
					}

					if birthdate != "" {
						insertTherapy := `insert into parse_vimis_semd.newborn 
					(
							date_start,
							date_end,
							ishod_berem_code,
							ishod_berem_name,
							lastname,
							patronymic,
							birthdate,
							number_in_birth_code,
							number_in_birth_name,
							gender_code,
							gender_name,
							mesto_rozhd_code,
							mesto_rozhd_name,
							donoshennost_code,
							donoshennost_name,
							sposob_rodorazresh_code,
							sposob_rodorazresh_name,
							massa_tela,
							dlina_tela,
							okruzh_golovy,
							okruzh_plech,
							rf_vimis_history_id,
							rezus_code,
							rezus_name,
							group_krov_code,
							group_krov_name
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
							$18,
							$19,
							$20,
							$21,
							$22,
							$23,
							$24,
							$25,
							$26

					) returning id`

						rows1, err := dbmap[dbname].Query(insertTherapy,

							NewNullDate(dateStart),
							NewNullDate(dateEnd),
							ishodcode,
							ishodename,
							lastname,
							patronymic,
							NewNullDate(birthdate),
							plodnumcode,
							plodnumname,
							gendercode,
							gendername,
							mestorozhcode,
							mestorozhname,
							donoshencode,
							donoshenname,
							sposobrodorazrcode,
							sposobrodorazrname,
							massa,
							dlina,
							okrgol,
							okrpl,
							_respStruct.Id,
							rezuscode,
							rezusname,
							groupcode,
							groupname,
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

						fmt.Println("newborn inserted :", Id)

						err = rows1.Close()
						if err != nil {
							fmt.Println(err)
						}
					}
				}
			}

			updateQueryText := `update logging_vimis.vimis_history set newborn_parsed_datetime = now() where id = $1`

			rows1, err := dbmap[dbname].Query(updateQueryText, _respStruct.Id)
			if err != nil {
				fmt.Println(err)
			}
			fmt.Println("Newborn Updated history:", _respStruct.Id)

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
