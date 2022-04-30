drop table if exists `db_name.weight_icustay_detail_modify_eicu`;
create table `db_name.weight_icustay_detail_modify_eicu` as

-- icustay_detail's admissionweight and dischargeweight modify
-- some admissionweight and dischargeweight exist errors (such as very small as big, admission_weight << discharge_weight,  admission_weight >> discharge_weight)
-- the values of 1, 20, 300, 0.15, 6.9 just check all of the outliers (get it)
with weight_info_10 as (
  select patientunitstayid
  , admissionweight
  , dischargeweight
  , round(admissionweight/dischargeweight,2) as ratio -- ratio <= 0.15 | ratio > 6.9 -- error
  from `physionet-data.eicu_crd_derived.icustay_detail`
  where admissionweight > 0
  and dischargeweight > 0
)

, weight_info_1 as (
  select patientunitstayid
  , case 
  when admissionweight <= 20 and admissionweight > 1 and ratio <= 0.15 then admissionweight*10
  when admissionweight <= 1 and ratio <= 0.15 then admissionweight*100
  when admissionweight >= 300 and ratio > 6.9 then admissionweight/10 
  else admissionweight end as admissionweight
  , case
  when dischargeweight >= 300 and ratio <= 0.15 then dischargeweight/10
  when dischargeweight >= 1000 and ratio <= 0.15 then dischargeweight/100
  when dischargeweight <= 20 and ratio > 6.9 then null
  else dischargeweight end as dischargeweight
  from weight_info_10
)

, weight_info_2 as (
  select patientunitstayid
  , admissionweight
  , dischargeweight
  from `physionet-data.eicu_crd_derived.icustay_detail`
  where dischargeweight is null
)

, weight_info_3 as (
  select patientunitstayid
  , admissionweight
  , dischargeweight
  from `physionet-data.eicu_crd_derived.icustay_detail`
  where admissionweight is null
)

, weight_info_4 as (
	select distinct *
	from (
		select *
		from weight_info_1
		union all
		select *
		from weight_info_2
		union all
		select *
		from weight_info_3
	)
)

-- admissionweight exists values of 0 and <2.5 were error
-- patientunitstayid = 1355321, 2387379 
, weight_info_5 as (
	select distinct *
	from (
		select patientunitstayid
		, case 
		when admissionweight < 2.5 then null
		else admissionweight end as admissionweight
		, dischargeweight
		from weight_info_4
		where patientunitstayid not in (1355321, 2387379)
		union all
		select patientunitstayid
		, dischargeweight as admissionweight
		, dischargeweight
		from weight_info_4
		where patientunitstayid in (1355321, 2387379)
	)
)

select *
from weight_info_5
order by patientunitstayid;