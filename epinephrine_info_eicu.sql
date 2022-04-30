drop table if exists `db_name.epinephrine_info_eicu`;
create table `db_name.epinephrine_info_eicu` as
--By Xiaoli Liu 
--2018.12.20 & 20201.09.07

-- get all of epinephrine information with drugrate units of mcg/kg/min
 
-- 1. add information of unit with 'Epinephrine ()'
-- process of 'Epinephrine ()' |  1879
-- we notice that 'drugname = 'Epinephrine ()'' might happen when :
-- 1) no value -- 0; 2) lose unit for saving time; 3) no-known reasons
-- so : we will solve the 2) by adding units considering the units before and after
with infusiondrug_new_0 as (
  select infusiondrugid, patientunitstayid, infusionoffset, drugname
  , cast(drugrate as numeric) as drugrate, infusionrate, drugamount, volumeoffluid, patientweight
  from `physionet-data.eicu_crd.infusiondrug`
  where 
  (
    drugname like 'Epinephrine%'
    or drugname like 'EPINEPHrine%'
  )
  and drugrate not like '%OFF%'
  and drugrate not like '%50%mcg%'
  and drugrate not like '%br%'
  and drugrate != ''
)

, epinephrine_in_part_0 as (
  select patientunitstayid
  from infusiondrug_new_0
  where drugname = 'Epinephrine ()'  -- |  24793
  group by patientunitstayid
)

, epinephrine_in_part_1 as (
  select ifd.infusiondrugid
  , ifd.patientunitstayid
  , ifd.infusionoffset
  , ifd.drugname
  , case 
  when ifd.drugname = 'Epinephrine' then 1
  else null end as unit_flag   -- the imputation function is fit for float value
  from infusiondrug_new_0 ifd
  inner join epinephrine_in_part_0 nip
  on ifd.patientunitstayid = nip.patientunitstayid
  where ifd.drugname like 'Epinephrine%'
)

, epinephrine_in_part_2 as (
  select nip.infusiondrugid
  , nip.patientunitstayid
  , nip.infusionoffset
  , nip.drugname 
  , nip.unit_flag
  , LAST_VALUE(nip.unit_flag IGNORE NULLS) OVER (partition by nip.patientunitstayid order by infusionoffset) as unit_flag_locf
  , LAST_VALUE(nip.unit_flag IGNORE NULLS) OVER (partition by nip.patientunitstayid order by infusionoffset desc) as unit_flag_focb
  from epinephrine_in_part_1 nip
)

, epinephrine_in_part_3 as (
  select nip.infusiondrugid
  , nip.patientunitstayid
  , nip.infusionoffset
  , nip.drugname 
  , nip.unit_flag
  , coalesce(nip.unit_flag_locf, nip.unit_flag_focb) as unit_flag_new
  from epinephrine_in_part_2 nip 
)

, epinephrine_in_part_4 as (
  select nip.infusiondrugid
  , nip.patientunitstayid
  , nip.infusionoffset
  , nip.drugname 
  , nip.unit_flag
  , case
  when nip.unit_flag_new = 1 then 'Epinephrine'
  else null end as drugname_new 
  from epinephrine_in_part_3 nip  
)

, epinephrine_in_part_5 as ( -- exist the units of epinephrine ()
  select nip.infusiondrugid
  , nip.patientunitstayid
  , nip.infusionoffset
  , nip.drugname_new as drugname
  , ifd.drugrate
  , ifd.infusionrate
  , ifd.drugamount
  , ifd.volumeoffluid
  , ifd.patientweight
  from epinephrine_in_part_4 nip
  inner join infusiondrug_new_0 ifd 
  on nip.infusiondrugid = ifd.infusiondrugid
)

, epinephrine_in_part_6 as (
  select ifd.infusiondrugid
  , ifd.patientunitstayid
  , ifd.infusionoffset
  , ifd.drugname as drugname
  , ifd.drugrate
  , ifd.infusionrate
  , ifd.drugamount
  , ifd.volumeoffluid
  , ifd.patientweight
  from infusiondrug_new_0 ifd
  where ifd.drugname like 'Epinephrine%'
  and ifd.patientunitstayid not in (select * from epinephrine_in_part_0)
)

, epinephrine_in_part as (
  select distinct *
  from (
    select *
    from epinephrine_in_part_5
    union all
    select *
    from epinephrine_in_part_6
  )
)

-- 2. Unified unit to mcg/kg/min
, epinephrine_1 as (
  select idn.infusiondrugid
  , idn.patientunitstayid
  , idn.infusionoffset
  , idn.drugname
  , idn.infusionrate
  , idn.drugamount
  , idn.volumeoffluid
  , idn.patientweight 
  , case 
  when idn.drugname in (
    'EPINEPHrine(Adrenalin)STD 4 mg Sodium Chloride 0.9% 250 ml (mcg/min)'  -- |   347
    , 'EPINEPHrine(Adrenalin)STD 4 mg Sodium Chloride 0.9% 500 ml (mcg/min)'  -- |   3
    , 'EPINEPHrine(Adrenalin)MAX 30 mg Sodium Chloride 0.9% 250 ml (mcg/min)' -- |   223
    , 'Epinephrine (mcg/min)'   -- |  14660
    , 'EPINEPHrine(Adrenalin)STD 7 mg Sodium Chloride 0.9% 250 ml (mcg/min)'  -- |   4
    )
    then idn.drugrate/(coalesce(coalesce(wi.admissionweight, wi.dischargeweight),80)) -- median(admissionweight) = 80
  when idn.drugname = 'Epinephrine (mcg/kg/min)'  -- |  3101
    then idn.drugrate
  when idn.drugname = 'Epinephrine (mcg/hr)'  -- |  3
    then idn.drugrate/(60 * coalesce(coalesce(wi.admissionweight, wi.dischargeweight),80))
  when idn.drugname = 'Epinephrine (mg/kg/min)'  --  |  341
    then 1000*idn.drugrate
  when idn.drugname = 'Epinephrine (mg/hr)'  --  |  9
    then 1000*drugrate/(60 * coalesce(coalesce(wi.admissionweight, wi.dischargeweight),80))
  else null end as rate_epinephrine 
  from epinephrine_in_part idn 
  left join `db_name.weight_icustay_detail_modify_eicu` wi 
  on idn.patientunitstayid = wi.patientunitstayid
  where idn.drugname in (
    'EPINEPHrine(Adrenalin)STD 4 mg Sodium Chloride 0.9% 500 ml (mcg/min)'  -- |   3
    , 'EPINEPHrine(Adrenalin)MAX 30 mg Sodium Chloride 0.9% 250 ml (mcg/min)' -- |   223
    , 'Epinephrine (mcg/min)'   -- |  14660
    , 'EPINEPHrine(Adrenalin)STD 7 mg Sodium Chloride 0.9% 250 ml (mcg/min)'  -- |   4
    , 'Epinephrine (mcg/kg/min)'
    , 'Epinephrine (mcg/hr)'
    , 'Epinephrine (mg/kg/min)'
    , 'Epinephrine (mg/hr)'
    )
)


-- without considering : Epinephrine  |  910   no value
, epinephrine_2 as (
  select idn.infusiondrugid
  , idn.patientunitstayid
  , idn.infusionoffset
  , idn.drugname
  , idn.infusionrate
  , idn.drugamount
  , idn.volumeoffluid
  , idn.patientweight
  , case
  when md.drugname = 'EPINEPHrine 8 MG in 250 mL NS' and idn.drugname = 'Epinephrine (ml/hr)'  -- |  33272
    then 1000*idn.drugrate*8/(250*60*coalesce(coalesce(wi.admissionweight, wi.dischargeweight),80))
  else 1000*idn.drugrate*4/(250*60*coalesce(coalesce(wi.admissionweight, wi.dischargeweight),80))
  end as rate_epinephrine
  from epinephrine_in_part idn 
  left join `physionet-data.eicu_crd.medication` md
  on idn.patientunitstayid = md.patientunitstayid
  and md.drugordercancelled = 'No'
  and md.drugname like 'Epinephrine%'
  left join `db_name.weight_icustay_detail_modify_eicu` wi 
  on idn.patientunitstayid = wi.patientunitstayid 
  where idn.drugname = 'Epinephrine (ml/hr)'  -- |  33272
  and idn.infusionoffset >= md.drugstartoffset
  and idn.infusionoffset <= md.drugstopoffset
)

, epinephrine as (
  select distinct *
  from (
      select *
      from epinephrine_1
      union all
      select *
      from epinephrine_2
  )
)

select infusiondrugid
, patientunitstayid
, infusionoffset
, drugname
, round(rate_epinephrine,4) as rate_epinephrine
, infusionrate
, drugamount
, volumeoffluid
, patientweight 
from epinephrine
order by patientunitstayid
, infusionoffset;