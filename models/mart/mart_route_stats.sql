WITH flight_route_stats AS (
				SELECT count(flight_number)  AS "NR OF connections"
						,dest 
						,origin
						, count (DISTINCT tail_number) AS "Unique planes"
						, count (DISTINCT airline) AS "Dif Airlines"
						,AVG ( actual_elapsed_time_interval)
						,AVG ( arr_delay_interval) AS AVG_arr_delay
						,AVG ( dep_delay_interval) AS dep_arr_delay
						,max ( arr_delay_interval) AS max_arr_delay
						,min ( arr_delay_interval) AS min_arr_delay
						,sum (cancelled) AS total_cancelled
						,sum (diverted) AS total_diverted
				FROM {{ref('prep_flights')}}        
				GROUP BY (dest, origin)
)
SELECT o.city AS origin_city, 
		d.city AS dest_city,
		o.name AS origin_name,
		d.name AS dest_name,
		o.country AS origin_country, 
		d.country AS dest_country,
		f.*
FROM flight_route_stats f
LEFT JOIN {{ref('prep_airports')}} o   
	ON f.origin=o.faa
LEFT JOIN {{ref('prep_airports')}} d
	ON f.dest=d.faa