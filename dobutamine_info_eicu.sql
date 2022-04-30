drop table if exists `db_name.team8.dobutamine_info_eicu`;
create table `db_name.team8.dobutamine_info_eicu` as
--By Xiaoli Liu 
--2018.12.21 & 2021.09.07

-- Because using dobutamine drug, will be considered as very serious, no needing to consider value or unit
with infusiondrug_new_0 as (
  select infusiondrugid, patientunitstayid, infusionoffset, drugname
  , cast(drugrate as numeric) as drugrate, infusionrate, drugamount, volumeoffluid, patientweight
  from `physionet-data.eicu_crd.infusiondrug`
  where (
    drugname like '%Dobutamine%'
    or drugname like '%DOBUTamine%'
  )
  and drugrate != ''
)

select infusiondrugid
, patientunitstayid
, infusionoffset
, drugname
, round(drugrate,4) as rate_dobutamine
, infusionrate
, drugamount
, volumeoffluid
, patientweight 
from infusiondrug_new_0
order by patientunitstayid
, infusionoffset;