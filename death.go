package main

import (
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/antchfx/xmlquery"
)

func parse_death() {

	for dbname := range dbmap {

		var responsesquery string

		responsesquery = `SELECT id, document FROM logging_vimis."vimis_history" vh
			where death_parsed_datetime is null and doc_type in ('13', '19')
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
									set death_parsed_datetime = now(),
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

			fmt.Println("death Id:", _respStruct.Id)
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
									set death_parsed_datetime = now(),
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

			var deathdate string

			deathdateArea := xmlquery.FindOne(doc, "//code[@code='521'][@codeSystem='1.2.643.5.1.13.13.99.2.166']")
			if deathdateArea != nil {
				if deathdateArea.Parent.SelectElement("value") != nil {
					deathdate = deathdateArea.Parent.SelectElement("value").SelectAttr("value")
				}
			}

			var rodCode, rodName *string
			rodArea := xmlquery.FindOne(doc, "//value[@codeSystem='1.2.643.5.1.13.13.99.2.21']")
			if rodArea != nil {
				rodCode = RefReturn(rodArea.SelectAttr("code"))
				rodName = RefReturn(rodArea.SelectAttr("displayName"))
			}

			var externDate string
			externDateArea := xmlquery.FindOne(doc, "//code[@codeSystem='1.2.643.5.1.13.13.99.2.166'][@code='6019']")
			if externDateArea != nil {
				externDate = externDateArea.Parent.SelectElement("value").SelectAttr("value")
			}

			var doctorPosCode, doctorPosName *string
			doctorPosArea := xmlquery.FindOne(doc, "//value[@codeSystem='1.2.643.5.1.13.13.99.2.22']")
			if doctorPosArea != nil {
				doctorPosCode = RefReturn(doctorPosArea.SelectAttr("code"))
				doctorPosName = RefReturn(doctorPosArea.SelectAttr("displayName"))
			}

			var groundCode, groudName *string
			groundArea := xmlquery.FindOne(doc, "//value[@codeSystem='1.2.643.5.1.13.13.99.2.23']")
			if groundArea != nil {
				groundCode = RefReturn(groundArea.SelectAttr("code"))
				groudName = RefReturn(groundArea.SelectAttr("displayName"))
			}

			var causeCode, causeName *string
			var causeDate string
			causeArea := xmlquery.FindOne(doc, "//code[@codeSystem='1.2.643.5.1.13.13.99.2.166'][@code='4030']")
			if causeArea != nil {
				causeCode = RefReturn(causeArea.Parent.SelectElement("value").SelectAttr("code"))
				causeName = RefReturn(causeArea.Parent.SelectElement("value").SelectAttr("displayName"))
				dateArea := causeArea.Parent.SelectElement("effectiveTime")
				if dateArea != nil {
					causeDate = dateArea.SelectAttr("value")
				}
			}

			var patologyCode, patologyName *string
			var patologyDate string
			patologyArea := xmlquery.FindOne(doc, "//code[@codeSystem='1.2.643.5.1.13.13.99.2.166'][@code='4035']")
			if patologyArea != nil {
				patologyCode = RefReturn(patologyArea.Parent.SelectElement("value").SelectAttr("code"))
				patologyName = RefReturn(patologyArea.Parent.SelectElement("value").SelectAttr("displayName"))
				dateArea := causeArea.Parent.SelectElement("effectiveTime")
				if dateArea != nil {
					patologyDate = dateArea.SelectAttr("value")
				}
			}

			var origCauseCode, origCauseName *string
			origCauseArea := xmlquery.FindOne(doc, "//code[@codeSystem='1.2.643.5.1.13.13.99.2.166'][@code='4040']")
			if patologyArea != nil {
				origCauseCode = RefReturn(origCauseArea.Parent.SelectElement("value").SelectAttr("code"))
				origCauseName = RefReturn(origCauseArea.Parent.SelectElement("value").SelectAttr("displayName"))

			}

			var externCauseCode, externCauseName *string
			externCauseArea := xmlquery.FindOne(doc, "//code[@codeSystem='1.2.643.5.1.13.13.99.2.166'][@code='4045']")
			if externCauseArea != nil {
				externCauseCode = RefReturn(externCauseArea.Parent.SelectElement("value").SelectAttr("code"))
				externCauseName = RefReturn(externCauseArea.Parent.SelectElement("value").SelectAttr("displayName"))

			}

			var pregnancyCode, pregnancyName *string
			pregnancyArea := xmlquery.FindOne(doc, "//code[@codeSystem='1.2.643.5.1.13.13.99.2.166'][@code='591']")
			if pregnancyArea != nil {
				pregnancyCode = RefReturn(pregnancyArea.Parent.SelectElement("value").SelectAttr("code"))
				pregnancyName = RefReturn(pregnancyArea.Parent.SelectElement("value").SelectAttr("displayName"))

			}

			var dtpCode, dtpName *string
			dtpArea := xmlquery.FindOne(doc, "//code[@codeSystem='1.2.643.5.1.13.13.99.2.166'][@code='601']")
			if dtpArea != nil {
				dtpCode = RefReturn(dtpArea.Parent.SelectElement("value").SelectAttr("code"))
				dtpName = RefReturn(dtpArea.Parent.SelectElement("value").SelectAttr("displayName"))

			}

			//var  *string

			if deathdate != "" {

				insertTherapy := `insert into parse_vimis_semd.death 
				(
					death_datetime,
					type_code,
					type_name,
					death_datetime_extern,
					determined_doctor_type_code,
					determined_doctor_type_name,
					ground_code,
					ground_name,
					cause_code,
					cause_name,
					cause_begin_datetime,
					cause_patology_code,
					cause_patology_name,
					cause_patology_begin_datetime,
					original_cause_code,
					original_cause_name,
					extern_cause_code,
					extern_cause_name,
					pregnancy_code,
					pregnancy_name,
					dtp_code,
					dtp_name,
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
						$18,
						$19,
						$20,
						$21,
						$22,
						$23

				) returning id`

				rows1, err := dbmap[dbname].Query(insertTherapy,

					NewNullDate(deathdate),
					rodCode,
					rodName,
					NewNullDate(externDate),
					doctorPosCode,
					doctorPosName,
					groundCode,
					groudName,
					causeCode,
					causeName,
					NewNullDate(causeDate),
					patologyCode,
					patologyName,
					NewNullDate(patologyDate),
					origCauseCode,
					origCauseName,
					externCauseCode,
					externCauseName,
					pregnancyCode,
					pregnancyName,
					dtpCode,
					dtpName,
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

				fmt.Println("death inserted :", Id)

				err = rows1.Close()
				if err != nil {
					fmt.Println(err)
				}
			}

			updateQueryText := `update logging_vimis.vimis_history set death_parsed_datetime = now() where id = $1`

			rows1, err := dbmap[dbname].Query(updateQueryText, _respStruct.Id)
			if err != nil {
				fmt.Println(err)
			}
			fmt.Println("death Updated history:", _respStruct.Id)

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
