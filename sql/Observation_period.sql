Truncate table @cdmDatabaseSchema.observation_period;
INSERT INTO @cdmDatabaseSchema.observation_period
	( person_id, observation_period_start_date, observation_period_end_date, period_type_concept_id)
	
select person_id, case
					when min(start_date) < '2017-01-01T00:00:0'  then '2017-01-01T00:00:0' 
					else min(start_date)
					end start_date, 
				  case
					when max(start_date) < '2017-01-01T00:00:0'  then '2017-01-01T00:00:0' 
					else max(start_date)
					end  end_date, 44814725
			from(
			select person_id, min(visit_start_date) start_date , max(visit_end_date) end_date  from @cdmDatabaseSchema.visit_occurrence group by person_id
			union
			select person_id, min(condition_start_date) start_date, 
				case
					 when max(condition_end_date) is null then '2022-01-01' 
					 else max(condition_end_date)
				 end end_date 
				 from @cdmDatabaseSchema.condition_occurrence  group by person_id
			union
			select person_id, min(procedure_date) start_date, 
				case
					 when max(procedure_date) is null then '2022-01-01' 
					 else max(procedure_date)
				 end end_date 
				 from @cdmDatabaseSchema.procedure_occurrence  group by person_id
			union
			select person_id, min(drug_exposure_start_date) start_date, 
				case
					 when max(drug_exposure_end_date) is null then '2022-01-01' 
					 else max(drug_exposure_end_date)
				 end end_date 
				 from @cdmDatabaseSchema.drug_exposure  group by person_id
			union
			select person_id, min(measurement_date) start_date, 
				case
					 when max(measurement_date) is null then '2022-01-01' 
					 else max(measurement_date)
				 end end_date 
				 from @cdmDatabaseSchema.measurement  group by person_id

						) as a group by a.person_id