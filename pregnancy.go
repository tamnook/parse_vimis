package main

import (
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/antchfx/xmlquery"
)

func parse_pregnancy() {

	for dbname := range dbmap {

		var responsesquery string

		responsesquery = `SELECT id, document FROM logging_vimis."vimis_history" vh
			where pregnancy_parsed_datetime is null --and doc_type = '32'
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
									set pregnancy_parsed_datetime = now(),
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

			fmt.Println("pregnancy Id:", _respStruct.Id)
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
									set pregnancy_parsed_datetime = now(),
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

			var srok *string
			srokArea := xmlquery.FindOne(doc, "//code[@codeSystem='1.2.643.5.1.13.13.99.2.166'][@code='6077']")
			if srokArea != nil {
				if srokArea.Parent.SelectElement("value") != nil {
					srok = RefReturn(AddDay(srokArea.Parent.SelectElement("value").SelectAttr("value")))
				}
			} else {
				srokArea = xmlquery.FindOne(doc, "//code[@codeSystem='1.2.643.5.1.13.13.11.1380'][@code='6077']")
				if srokArea != nil {
					if srokArea.Parent.SelectElement("value") != nil {
						srok = RefReturn(AddDay(srokArea.Parent.SelectElement("value").SelectAttr("value")))
					}
				} else {
					srokArea = xmlquery.FindOne(doc, "//code[@codeSystem='1.2.643.5.1.13.13.99.2.166'][@code='2010']")
					if srokArea != nil {
						if srokArea.Parent.SelectElement("value") != nil {
							srok = RefReturn(AddWeek(srokArea.Parent.SelectElement("value").SelectAttr("value")))
						}
					} else {
						srokArea = xmlquery.FindOne(doc, "//code[@codeSystem='1.2.643.5.1.13.13.11.1380'][@code='2010']")
						if srokArea != nil {
							if srokArea.Parent.SelectElement("value") != nil {
								srok = RefReturn(AddWeek(srokArea.Parent.SelectElement("value").SelectAttr("value")))
							}
						}
					}
				}
			}

			var countPlod *string
			countPlodArea := xmlquery.FindOne(doc, "//code[@codeSystem='1.2.643.5.1.13.13.99.2.166'][@code='6078']")
			if countPlodArea != nil {
				if countPlodArea.Parent.SelectElement("value") != nil {
					countPlod = RefReturn(countPlodArea.Parent.SelectElement("value").SelectAttr("value"))
				}
			} else {
				countPlodArea = xmlquery.FindOne(doc, "//code[@codeSystem='1.2.643.5.1.13.13.11.1380'][@code='6078']")
				if countPlodArea != nil {
					if countPlodArea.Parent.SelectElement("value") != nil {
						countPlod = RefReturn(countPlodArea.Parent.SelectElement("value").SelectAttr("value"))
					}
				}
			}

			var pregNum *string
			pregNumArea := xmlquery.FindOne(doc, "//code[@codeSystem='1.2.643.5.1.13.13.99.2.166'][@code='12310']")
			if pregNumArea != nil {
				if pregNumArea.Parent.SelectElement("value") != nil {
					pregNum = RefReturn(pregNumArea.Parent.SelectElement("value").SelectAttr("value"))
				}
			} else {
				pregNumArea = xmlquery.FindOne(doc, "//code[@codeSystem='1.2.643.5.1.13.13.11.1380'][@code='12310']")
				if pregNumArea != nil {
					if pregNumArea.Parent.SelectElement("value") != nil {
						pregNum = RefReturn(pregNumArea.Parent.SelectElement("value").SelectAttr("value"))
					}
				}
			}

			var birthNum *string
			birthNumArea := xmlquery.FindOne(doc, "//code[@codeSystem='1.2.643.5.1.13.13.99.2.166'][@code='5002']")
			if birthNumArea != nil {
				if birthNumArea.Parent.SelectElement("value") != nil {
					birthNum = RefReturn(birthNumArea.Parent.SelectElement("value").SelectAttr("value"))
				}
			} else {
				birthNumArea = xmlquery.FindOne(doc, "//code[@codeSystem='1.2.643.5.1.13.13.11.1380'][@code='5002']")
				if birthNumArea != nil {
					if birthNumArea.Parent.SelectElement("value") != nil {
						birthNum = RefReturn(birthNumArea.Parent.SelectElement("value").SelectAttr("value"))
					}
				}
			}

			var zachatTypeCode, zachatTypeName *string
			zachatTypeArea := xmlquery.FindOne(doc, "//value[@codeSystem='1.2.643.5.1.13.13.99.2.404']")
			if zachatTypeArea != nil {
				zachatTypeCode = RefReturn(zachatTypeArea.SelectAttr("code"))
				zachatTypeName = RefReturn(zachatTypeArea.SelectAttr("displayName"))
			}

			var plannedDate string
			plannedDateArea := xmlquery.FindOne(doc, "//code[@codeSystem='1.2.643.5.1.13.13.99.2.166'][@code='12368']")
			if plannedDateArea != nil {
				if plannedDateArea.Parent.SelectElement("effectiveTime") != nil {
					plannedDate = plannedDateArea.Parent.SelectElement("effectiveTime").SelectAttr("value")
				}
			} else {
				plannedDateArea = xmlquery.FindOne(doc, "//code[@codeSystem='1.2.643.5.1.13.13.11.1380'][@code='12368']")
				if plannedDateArea != nil {
					if plannedDateArea.Parent.SelectElement("effectiveTime") != nil {
						plannedDate = plannedDateArea.Parent.SelectElement("effectiveTime").SelectAttr("value")
					}
				}
			}

			if plannedDate == "" {
				plannedDateArea = xmlquery.FindOne(doc, "//th[text()='Предполагаемая дата родов']")
				if plannedDateArea != nil {
					// fmt.Println("//th[text()='Предполагаемая дата родов']")
					if plannedDateArea.Parent.SelectElement("td") != nil {
						plannedDate = plannedDateArea.Parent.SelectElement("td").InnerText()
						// fmt.Println(plannedDateArea.Parent.SelectElement("td").InnerText())
					}
				}
			}

			var uchetSrokCode, uchetSrokName *string
			var uchetDate string
			uchetDateArea := xmlquery.FindOne(doc, "//code[@codeSystem='1.2.643.5.1.13.13.99.2.425']")
			if uchetDateArea != nil {
				uchetSrokCode = RefReturn(uchetDateArea.SelectAttr("code"))
				uchetSrokName = RefReturn(uchetDateArea.SelectAttr("displayName"))
				if uchetDateArea.Parent.SelectElement("effectiveTime") != nil {
					uchetDate = uchetDateArea.Parent.SelectElement("effectiveTime").SelectAttr("value")
				}
			}

			var menstrDate string
			menstrDateArea := xmlquery.FindOne(doc, "//code[@codeSystem='1.2.643.5.1.13.13.99.2.166'][@code='12363']")
			if menstrDateArea != nil {
				if menstrDateArea.Parent.SelectElement("effectiveTime") != nil {
					menstrDate = menstrDateArea.Parent.SelectElement("effectiveTime").SelectAttr("value")
				}
			} else {
				menstrDateArea = xmlquery.FindOne(doc, "//code[@codeSystem='1.2.643.5.1.13.13.11.1380'][@code='12363']")
				if menstrDateArea != nil {
					if menstrDateArea.Parent.SelectElement("effectiveTime") != nil {
						menstrDate = menstrDateArea.Parent.SelectElement("effectiveTime").SelectAttr("value")
					}
				}
			}

			if menstrDate == "" {
				menstrDateArea = xmlquery.FindOne(doc, "//th[text()='Первый день последней менструации']")
				if menstrDateArea != nil {
					if menstrDateArea.Parent.SelectElement("td") != nil {
						menstrDate = menstrDateArea.Parent.SelectElement("td").InnerText()
					}
				}
			}

			var massaBerem *string
			massaBeremArea := xmlquery.FindOne(doc, "//code[@codeSystem='1.2.643.5.1.13.13.99.2.166'][@code='12364']")
			if massaBeremArea != nil {
				if massaBeremArea.Parent.SelectElement("value") != nil {
					massaBerem = RefReturn(massaBeremArea.Parent.SelectElement("value").SelectAttr("value"))
				}
			} else {
				massaBeremArea = xmlquery.FindOne(doc, "//code[@codeSystem='1.2.643.5.1.13.13.11.1380'][@code='12364']")
				if massaBeremArea != nil {
					if massaBeremArea.Parent.SelectElement("value") != nil {
						massaBerem = RefReturn(massaBeremArea.Parent.SelectElement("value").SelectAttr("value"))
					}
				}
			}

			if massaBerem == nil {
				massaBeremArea = xmlquery.FindOne(doc, "//th[text()='Масса тела беременной (до беременности)']")
				if massaBeremArea != nil {
					if massaBeremArea.Parent.SelectElement("td") != nil {
						massaBerem = RefReturn(massaBeremArea.Parent.SelectElement("td").InnerText())
					}
				}
			}

			var highRisk *string

			highRiskArea := xmlquery.FindOne(doc, "//tr/th/content[text()='Степень риска беременной']")
			if highRiskArea != nil {
				//fmt.Println("//tr/th/content[text()='Степень риска беременной']", highRiskArea.Parent.Parent.OutputXML(true))
				if highRiskArea.Parent.Parent.SelectElement("td") != nil {
					if highRiskArea.Parent.Parent.SelectElement("td").SelectElement("content") != nil {
						highRisk = RefReturn(highRiskArea.Parent.Parent.SelectElement("td").SelectElement("content").InnerText())
						//fmt.Println("//tr/th/content[text()='Степень риска беременной']", highRiskArea.Parent.Parent.SelectElement("td").SelectElement("content").InnerText())
					}
				}
			}

			var ishCode, ishName, countNewborn *string
			var ishDate string
			ishArea := xmlquery.FindOne(doc, "//code[@codeSystem='1.2.643.5.1.13.13.99.2.166'][@code='310']")
			if ishArea != nil {
				ishCode = RefReturn("1")
				ishName = RefReturn("Рождение ребенка")
				if ishArea.Parent.SelectElement("value") != nil {
					countNewborn = RefReturn(ishArea.Parent.SelectElement("value").SelectAttr("value"))
				}
				srokArea := xmlquery.FindOne(ishArea, "//code[@codeSystem='1.2.643.5.1.13.13.99.2.166'][@code='6077']")
				if srokArea != nil {
					if srokArea.Parent.SelectElement("value") != nil {
						srok = RefReturn(AddDay(srokArea.Parent.SelectElement("value").SelectAttr("value")))
					}
				} else {
					srokArea = xmlquery.FindOne(ishArea, "//code[@codeSystem='1.2.643.5.1.13.13.99.2.166'][@code='2010']")
					if srokArea != nil {
						if srokArea.Parent.SelectElement("value") != nil {
							srok = RefReturn(AddWeek(srokArea.Parent.SelectElement("value").SelectAttr("value")))
						}
					}
				}

				ishDateArea := xmlquery.FindOne(doc, "//code[@codeSystem='1.2.643.5.1.13.13.11.1021'][@code='2']")
				if ishDateArea != nil {
					if xmlquery.FindOne(ishDateArea.Parent, "//birthTime") != nil {
						ishDate = xmlquery.FindOne(ishDateArea.Parent, "//birthTime").SelectAttr("value")
					}
				}

			} else {
				ishArea := xmlquery.FindOne(doc, "//code[@codeSystem='1.2.643.5.1.13.13.11.1380'][@code='310']")
				if ishArea != nil {
					ishCode = RefReturn("1")
					ishName = RefReturn("Рождение ребенка")
					countNewborn = RefReturn(ishArea.Parent.SelectElement("value").SelectAttr("value"))
					srokArea := xmlquery.FindOne(ishArea, "//code[@codeSystem='1.2.643.5.1.13.13.11.1380'][@code='6077']")
					if srokArea != nil {
						if srokArea.Parent.SelectElement("value") != nil {
							srok = RefReturn(AddDay(srokArea.Parent.SelectElement("value").SelectAttr("value")))
						}
					} else {
						srokArea = xmlquery.FindOne(ishArea, "//code[@codeSystem='1.2.643.5.1.13.13.11.1380'][@code='2010']")
						if srokArea != nil {
							if srokArea.Parent.SelectElement("value") != nil {
								srok = RefReturn(AddWeek(srokArea.Parent.SelectElement("value").SelectAttr("value")))
							}
						}
					}

					ishDateArea := xmlquery.FindOne(doc, "//code[@codeSystem='1.2.643.5.1.13.13.11.1021'][@code='2']")
					if ishDateArea != nil {
						if xmlquery.FindOne(ishDateArea.Parent, "//birthTime") != nil {
							ishDate = xmlquery.FindOne(ishDateArea.Parent, "//birthTime").SelectAttr("value")
						}
					}

				} else {
					ishArea = xmlquery.FindOne(doc, "//code[@codeSystem='1.2.643.5.1.13.13.99.2.279']")
					if ishArea != nil {
						ishCode = RefReturn(ishArea.SelectAttr("code"))
						ishName = RefReturn(ishArea.SelectAttr("displayName"))

						ishDate = ishArea.Parent.SelectElement("effectiveTime").SelectAttr("value")
						srokArea := xmlquery.FindOne(ishArea, "//code[@codeSystem='1.2.643.5.1.13.13.99.2.166'][@code='6077']")
						if srokArea != nil {
							if srokArea.Parent.SelectElement("value") != nil {
								srok = RefReturn(AddDay(srokArea.Parent.SelectElement("value").SelectAttr("value")))
							}
						} else {
							srokArea := xmlquery.FindOne(ishArea, "//code[@codeSystem='1.2.643.5.1.13.13.11.1380'][@code='6077']")
							if srokArea != nil {
								if srokArea.Parent.SelectElement("value") != nil {
									srok = RefReturn(AddDay(srokArea.Parent.SelectElement("value").SelectAttr("value")))
								}
							} else {
								srokArea = xmlquery.FindOne(ishArea, "//code[@codeSystem='1.2.643.5.1.13.13.99.2.166'][@code='2010']")
								if srokArea != nil {
									if srokArea.Parent.SelectElement("value") != nil {
										srok = RefReturn(AddWeek(srokArea.Parent.SelectElement("value").SelectAttr("value")))
									}
								} else {
									srokArea = xmlquery.FindOne(ishArea, "//code[@codeSystem='1.2.643.5.1.13.13.11.1380'][@code='2010']")
									if srokArea != nil {
										if srokArea.Parent.SelectElement("value") != nil {
											srok = RefReturn(AddWeek(srokArea.Parent.SelectElement("value").SelectAttr("value")))
										}
									}
								}
							}
						}
					}
				}
			}

			if srok != nil || ishCode != nil || countPlod != nil || pregNum != nil || zachatTypeCode != nil || plannedDate != "" || uchetSrokCode != nil || uchetDate != "" || menstrDate != "" || massaBerem != nil || countNewborn != nil || ishDate != "" {
				insertTherapy := `insert into parse_vimis_semd.pregnancy 
				(
						srok,
						count_plod,
						pregnancy_num,
						birth_num,
						zachat_type_code,
						zachat_type_name,
						planned_date,
						uchet_srok_code,
						uchet_srok_name,
						uchet_date,
						last_menstr_date,
						massa_berem,
						ishod_code,
						ishod_name,
						count_newborn,
						ishod_date,
						rf_vimis_history_id,
						high_risk_name
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

				rows1, err := dbmap[dbname].Query(insertTherapy,
					srok,
					countPlod,
					pregNum,
					birthNum,
					zachatTypeCode,
					zachatTypeName,
					NewNullDate(plannedDate),
					uchetSrokCode,
					uchetSrokName,
					NewNullDate(uchetDate),
					NewNullDate(menstrDate),
					massaBerem,
					ishCode,
					ishName,
					countNewborn,
					NewNullDate(ishDate),
					_respStruct.Id,
					highRisk,
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

				fmt.Println("pregnancy inserted :", Id)

				err = rows1.Close()
				if err != nil {
					fmt.Println(err)
				}
			}

			updateQueryText := `update logging_vimis.vimis_history set pregnancy_parsed_datetime = now() where id = $1`

			rows1, err := dbmap[dbname].Query(updateQueryText, _respStruct.Id)
			if err != nil {
				fmt.Println(err)
			}
			fmt.Println("pregnancy Updated history:", _respStruct.Id)

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
