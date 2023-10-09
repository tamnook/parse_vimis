package main

/*import (
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/antchfx/xmlquery"
)

func parse_disp_uchet() {

	for dbname := range dbmap {

		var responsesquery string

		responsesquery = `SELECT id, document FROM logging_vimis."vimis_history" vh
			where du_parsed_datetime is null
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
									set du_parsed_datetime = now(),
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

			fmt.Println("du Id:", _respStruct.Id)
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
									set du_parsed_datetime = now(),
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

			MainArea := xmlquery.FindOne(doc, "//section[code[@code='vimisDispensaryObservation'][@codeSystem='vimis1']]")
			var dateStart, dateEnd, dateIzvesh, dateDeath string
			var statusCode, statusName, conditionCode, conditionName, autopsyCode, autopsyName, autopsyResCode, autopsyResName *string
			var dispOutReasonCode, dispOutReasonName, dispOutReasonClarCode, dispOutReasonClarName *string
			var lateDiagReasonCode, lateDiagReasonName *string
			if MainArea != nil {
				statusArea := xmlquery.FindOne(MainArea, "//code[@codeSystem='1.2.643.5.1.13.13.11.1047']")
				if statusArea != nil {
					statusCode = RefReturn(statusArea.SelectAttr("code"))
					statusName = RefReturn(statusArea.SelectAttr("displayName"))
				}

				dateArea := xmlquery.FindOne(MainArea.Parent, "//width[translation[@code='1']]")
				if dateArea != nil {
					dateStart = dateArea.SelectAttr("value")
				}

				dateArea = xmlquery.FindOne(MainArea.Parent, "//width[translation[@code='2']]")
				if dateArea != nil {
					dateIzvesh = dateArea.SelectAttr("value")
				}

				dateArea = xmlquery.FindOne(MainArea.Parent, "//width[translation[@code='3']]")
				if dateArea != nil {
					dateEnd = dateArea.SelectAttr("value")
				}

				dateArea = xmlquery.FindOne(MainArea.Parent, "//width[translation[@code='4']]")
				if dateArea != nil {
					dateDeath = dateArea.SelectAttr("value")
				}

				conditionArea := xmlquery.FindOne(MainArea.Parent, "//value[@codeSystem='1.2.643.5.1.13.13.99.2.140']")
				if conditionArea != nil {
					conditionCode = RefReturn(conditionArea.SelectAttr("code"))
					conditionName = RefReturn(conditionArea.SelectAttr("displayName"))
				}

				autopsyArea := xmlquery.FindOne(MainArea.Parent, "//value[@codeSystem='1.2.643.5.1.13.13.99.2.142']")
				if autopsyArea != nil {
					autopsyCode = RefReturn(autopsyArea.SelectAttr("code"))
					autopsyName = RefReturn(autopsyArea.SelectAttr("displayName"))
				}

				autopsyResArea := xmlquery.FindOne(MainArea.Parent, "//value[@codeSystem='1.2.643.5.1.13.13.99.2.587']")
				if autopsyResArea != nil {
					autopsyResCode = RefReturn(autopsyResArea.SelectAttr("code"))
					autopsyResName = RefReturn(autopsyResArea.SelectAttr("displayName"))
				}

				dispOutReasonArea := xmlquery.FindOne(MainArea.Parent, "//value[@codeSystem='1.2.643.5.1.13.13.11.1045']")
				if dispOutReasonArea != nil {
					dispOutReasonCode = RefReturn(dispOutReasonArea.SelectAttr("code"))
					dispOutReasonName = RefReturn(dispOutReasonArea.SelectAttr("displayName"))
				}

				dispOutReasonClarArea := xmlquery.FindOne(MainArea.Parent, "//value[@codeSystem='1.2.643.5.1.13.13.99.2.150']")
				if dispOutReasonClarArea != nil {
					dispOutReasonClarCode = RefReturn(dispOutReasonClarArea.SelectAttr("code"))
					dispOutReasonClarName = RefReturn(dispOutReasonClarArea.SelectAttr("displayName"))
				}

				lateDiagArea := xmlquery.FindOne(MainArea.Parent, "//value[@codeSystem='1.2.643.5.1.13.13.99.2.142']")
				if lateDiagArea != nil {
					autopsyCode = RefReturn(lateDiagArea.SelectAttr("code"))
					autopsyName = RefReturn(lateDiagArea.SelectAttr("displayName"))
				}
			}

			updateQueryText := `update logging_vimis.vimis_history set du_parsed_datetime = now() where id = $1`

			rows1, err := dbmap[dbname].Query(updateQueryText, _respStruct.Id)
			if err != nil {
				fmt.Println(err)
			}
			fmt.Println("Du Updated history:", _respStruct.Id)

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

}*/