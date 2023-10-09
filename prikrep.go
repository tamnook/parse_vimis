package main

import (
	"fmt"
)

func prikrep() {

	responsesquery := `SELECT distinct patient_snils FROM logging_vimis.vimis_control vc
			where prikrep_query_date_time is null or (now()::date - prikrep_query_date_time::date) > 30
			limit 10000
			`

	rows, err := dbmap["dbVimis"].Query(responsesquery)

	if err != nil {
		fmt.Println("dbVimis", err)
	}

	snils := make([]string, 0)

	for rows.Next() {
		var t string
		err = rows.Scan(&t)
		if err != nil {
			fmt.Println("dbVimis", err)
		} else {
			snils = append(snils, t)
		}
	}
	err = rows.Close()

	if err != nil {
		fmt.Println(err)
	}

	misQuery := `select distinct t2."name" from patient t0 left join lpu t2 on t2.id = t0.rf_lpu_id where replace(replace(t0.snils,'-',''),' ','') = $1 and t0.deleted = false`

	for _, v := range snils {
		rows, err = dbMis.Query(misQuery, v)
		if err != nil {
			fmt.Println(err)
			continue
		}
		for rows.Next() {
			var t *string
			err = rows.Scan(&t)
			if err != nil {
				fmt.Println("dbmis", err)

				continue
			} else {
				updateQueryText := `update logging_vimis.vimis_control set prikrep_query_date_time = now(), prikrep_mo_name = $2 where patient_snils = $1`

				rows1, err := dbmap["dbVimis"].Query(updateQueryText, v, t)
				if err != nil {
					fmt.Println(err)
					continue
				}
				fmt.Println("Prikrep Updated control:", v)

				err = rows1.Close()
				if err != nil {
					fmt.Println(err)
				}
			}
		}
		err = rows.Close()

		if err != nil {
			fmt.Println(err)
		}
	}
}
