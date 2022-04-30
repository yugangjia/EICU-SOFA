drop table if exists `db_name.dopamine_info_eicu`;
create table `db_name.dopamine_info_eicu` as

--By Xiaoli Liu 
--2018.12.20 & 2021.09.07

-- get all of dopamine information with drugrate units of mcg/kg/min
 
-- 1. add information of unit with 'Dopamine ()'
-- process of 'Dopamine ()' |  5020
-- we notice that 'drugname = 'Norepinephrine ()'' might happen when :
-- 1) no value -- 0; 2) lose unit for saving time; 3) no-known reasons
-- so : we will solve the 2) by adding units considering the units before and after
with infusiondrug_new_0 as (
  select infusiondrugid, patientunitstayid, infusionoffset, drugname
  , cast(drugrate as numeric) as drugrate, infusionrate, drugamount, volumeoffluid, patientweight
  from `physionet-data.eicu_crd.infusiondrug`
  where 
  (
    drugname like '%dopamine%'
    or drugname like '%Dopamine%'
    or drugname like '%DOPamine%'
  )
  and drugrate not in (
        'OFF'
        , 'ERROR'
        , 'UD'
        , ''
  )
  and drugrate not like '%Time Correction%'
)

, dopamine_in_part_0 as (
  select patientunitstayid
  from infusiondrug_new_0
  where drugname = 'Dopamine ()'  -- |  5020
  group by patientunitstayid
)

, dopamine_in_part_1 as (
  select ifd.infusiondrugid
  , ifd.patientunitstayid
  , ifd.infusionoffset
  , ifd.drugname
  , case 
  when ifd.drugname = 'Dopamine (mcg/kg/hr)' then 1
  when ifd.drugname = 'Dopamine (mcg/kg/min)' then 2
  when ifd.drugname = 'Dopamine (ml/hr)' then 3
  when ifd.drugname = 'Dopamine' then 4
  else null end as unit_flag   -- the imputation function is fit for float value
  from infusiondrug_new_0 ifd
  inner join dopamine_in_part_0 nip
  on ifd.patientunitstayid = nip.patientunitstayid
  where ifd.drugname like '%Dopamine%'
)

, dopamine_in_part_2 as (
  select nip.infusiondrugid
  , nip.patientunitstayid
  , nip.infusionoffset
  , nip.drugname 
  , nip.unit_flag
  , LAST_VALUE(nip.unit_flag IGNORE NULLS) OVER (partition by nip.patientunitstayid order by infusionoffset) as unit_flag_locf
  , LAST_VALUE(nip.unit_flag IGNORE NULLS) OVER (partition by nip.patientunitstayid order by infusionoffset desc) as unit_flag_focb
  from dopamine_in_part_1 nip
)

, dopamine_in_part_3 as (
  select nip.infusiondrugid
  , nip.patientunitstayid
  , nip.infusionoffset
  , nip.drugname 
  , nip.unit_flag
  , coalesce(nip.unit_flag_locf, nip.unit_flag_focb) as unit_flag_new
  from dopamine_in_part_2 nip 
)

, dopamine_in_part_4 as (
  select nip.infusiondrugid
  , nip.patientunitstayid
  , nip.infusionoffset
  , nip.drugname 
  , nip.unit_flag
  , case
  when nip.unit_flag_new = 1 then 'Dopamine (mcg/kg/hr)'
  when nip.unit_flag_new = 2 then 'Dopamine (mcg/kg/min)'
  when nip.unit_flag_new = 3 then 'Dopamine (ml/hr)'
  when nip.unit_flag_new = 4 then 'Dopamine'
  else null end as drugname_new 
  from dopamine_in_part_3 nip  
)

, dopamine_in_part_5 as ( -- exist the units of Dopamine ()
  select nip.infusiondrugid
  , nip.patientunitstayid
  , nip.infusionoffset
  , nip.drugname_new as drugname
  , ifd.drugrate
  , ifd.infusionrate
  , ifd.drugamount
  , ifd.volumeoffluid
  , ifd.patientweight
  from dopamine_in_part_4 nip
  inner join infusiondrug_new_0 ifd 
  on nip.infusiondrugid = ifd.infusiondrugid
)

, dopamine_in_part_6 as (
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
  where ifd.drugname like '%dopamine%'
  and ifd.patientunitstayid not in (select * from dopamine_in_part_0)
)

, dopamine_in_part as (
  select distinct *
  from (
    select *
    from dopamine_in_part_5
    union all 
    select *
    from dopamine_in_part_6
  )
)

-- 2. Unified unit to mcg/kg/min
, dopamine_1 as (
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
   'DOPamine MAX 800 mg Dextrose 5% 250 ml  Premix (mcg/kg/min)' --   | 	151
   , 'dopamine (mcg/kg/min)'   --   |  	21
   , 'DOPamine STD 15 mg Dextrose 5% 250 ml  Premix (mcg/kg/min)'  --  | 	2
   , 'Dopamine (mcg/kg/min)'  --  |	29215
   , 'DOPamine STD 400 mg Dextrose 5% 500 ml  Premix (mcg/kg/min)'  -- |  2 
   , 'DOPamine STD 400 mg Dextrose 5% 250 ml  Premix (mcg/kg/min)'  -- |  544
  )
    then idn.drugrate 
  when idn.drugname = 'Dopamine (mcg/kg/hr)'  --  |	 5
    then idn.drugrate/60 
  when idn.drugname = 'Dopamine (mcg/hr)'  --  |  8
    then idn.drugrate/(60 * coalesce(coalesce(wi.admissionweight, wi.dischargeweight),80))
  when idn.drugname = 'Dopamine (mcg/min)'  -- |   3
    then idn.drugrate/(coalesce(coalesce(wi.admissionweight, wi.dischargeweight),80))
  when idn.drugname = 'Dopamine (mg/hr)'  -- |	2
    then 1000 * idn.drugrate/(60 * coalesce(coalesce(wi.admissionweight, wi.dischargeweight),80))
  when idn.drugname = 'Dopamine (nanograms/kg/min)' -- |  1
    then idn.drugrate/1000
  else null end as rate_dopamine
  from dopamine_in_part idn
  left join `db_name.weight_icustay_detail_modify_eicu` wi 
  on idn.patientunitstayid = wi.patientunitstayid
  where idn.drugname in (
    'DOPamine MAX 800 mg Dextrose 5% 250 ml  Premix (mcg/kg/min)'      -- | 151 
    , 'dopamine (mcg/kg/min)'     -- | 21
    , 'DOPamine STD 15 mg Dextrose 5% 250 ml  Premix (mcg/kg/min)'    -- |  2
    , 'Dopamine (mcg/kg/min)'  -- |  29215
    , 'DOPamine STD 400 mg Dextrose 5% 500 ml  Premix (mcg/kg/min)'   -- |  2
    , 'DOPamine STD 400 mg Dextrose 5% 250 ml  Premix (mcg/kg/min)'   -- |  544
    , 'Dopamine (mcg/kg/hr)'          -- |  5
    , 'Dopamine (mcg/hr)'             -- |  8
    , 'Dopamine (mcg/min)'            -- |  3 
    , 'Dopamine (mg/hr)'             --  |  2
    , 'Dopamine (nanograms/kg/min)'  --  |   2
    )
)

-- without considering Dopamine	1684
, dopamine_2 as (
  select idn.infusiondrugid
  , idn.patientunitstayid
  , idn.infusionoffset
  , idn.drugname
  , idn.infusionrate
  , idn.drugamount
  , idn.volumeoffluid
  , idn.patientweight
  , 1000*idn.drugrate*4/(250*60*coalesce(coalesce(wi.admissionweight, wi.dischargeweight),80)) as rate_dopamine -- set mL available : 250ml, maybe not right 
  from dopamine_in_part idn
  left join `db_name.weight_icustay_detail_modify_eicu` wi 
  on idn.patientunitstayid = wi.patientunitstayid  
  where drugname = 'Dopamine (ml/hr)'  -- |  56099  
)	

, dopamine as (
    select distinct *
    from (
        select *
        from dopamine_1
        union all
        select *
        from dopamine_2
    )
)

select infusiondrugid
, patientunitstayid
, infusionoffset
, drugname
, round(rate_dopamine,4) as rate_dopamine
, infusionrate
, drugamount
, volumeoffluid
, patientweight 
from dopamine
order by patientunitstayid, infusionoffset;




