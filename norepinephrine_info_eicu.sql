drop table if exists `db_name.norepinephrine_info_eicu`;
create table `db_name.norepinephrine_info_eicu` as
-- get all of norepinephrine information with drugrate units of mcg/kg/min
 
-- 1. add information of unit with 'Norepinephrine ()'
-- process of 'Norepinephrine ()' |  24793
-- we notice that 'drugname = 'Norepinephrine ()'' might happen when :
-- 1) no value -- 0; 2) lose unit for saving time; 3) no-known reasons
-- so : we will solve the 2) by adding units considering the units before and after

with infusiondrug_new_0 as (
  select infusiondrugid, patientunitstayid, infusionoffset, drugname
  , cast(drugrate as numeric) as drugrate, infusionrate, drugamount, volumeoffluid, patientweight
  from `physionet-data.eicu_crd.infusiondrug`
  where (
    drugname like '%Norepinephrine%'
    or drugname like '%norepinephrine%'
  )
  and drugrate not in (
        'Documentation undone'
        , 'ERROR'
        , 'UD'
        , ''
  )
  and drugrate not like '%OFF%'
)

, norepinephrine_in_part_0 as (
  select patientunitstayid
  from infusiondrug_new_0
  where drugname = 'Norepinephrine ()'  -- |  24793
  group by patientunitstayid
)

, norepinephrine_in_part_1 as (
  select ifd.infusiondrugid
  , ifd.patientunitstayid
  , ifd.infusionoffset
  , ifd.drugname
  , case 
  when ifd.drugname = 'Norepinephrine (mcg/kg/hr)' then 1
  when ifd.drugname = 'Norepinephrine (mcg/kg/min)' then 2
  when ifd.drugname = 'Norepinephrine (mcg/min)' then 3
  when ifd.drugname = 'Norepinephrine (mg/hr)' then 4
  when ifd.drugname = 'Norepinephrine (mg/min)' then 5
  when ifd.drugname = 'Norepinephrine (ml/hr)' then 6
  when ifd.drugname like '%norepinephrine%' then 6
  when ifd.drugname = 'Norepinephrine' then 7 
  else null end as unit_flag   -- the imputation function is fit for float value
  from infusiondrug_new_0 ifd
  inner join norepinephrine_in_part_0 nip
  on ifd.patientunitstayid = nip.patientunitstayid
  where ifd.drugname like '%Norepinephrine%'
  or ifd.drugname like '%norepinephrine%'
)

-- https://stackoverflow.com/questions/41782630/google-big-query-forward-filling-ignore-in-window-function
, norepinephrine_in_part_2 as (
  select nip.infusiondrugid
  , nip.patientunitstayid
  , nip.infusionoffset
  , nip.drugname 
  , nip.unit_flag
  , LAST_VALUE(nip.unit_flag IGNORE NULLS) OVER (partition by nip.patientunitstayid order by infusionoffset) as unit_flag_locf
  , LAST_VALUE(nip.unit_flag IGNORE NULLS) OVER (partition by nip.patientunitstayid order by infusionoffset desc) as unit_flag_focb
  from norepinephrine_in_part_1 nip
)

, norepinephrine_in_part_3 as (
  select nip.infusiondrugid
  , nip.patientunitstayid
  , nip.infusionoffset
  , nip.drugname 
  , nip.unit_flag
  , coalesce(nip.unit_flag_locf, nip.unit_flag_focb) as unit_flag_new
  from norepinephrine_in_part_2 nip 
)

, norepinephrine_in_part_4 as (
  select nip.infusiondrugid
  , nip.patientunitstayid
  , nip.infusionoffset
  , nip.drugname 
  , nip.unit_flag
  , case
  when nip.unit_flag_new = 1 then 'Norepinephrine (mcg/kg/hr)'
  when nip.unit_flag_new = 2 then 'Norepinephrine (mcg/kg/min)'
  when nip.unit_flag_new = 3 then 'Norepinephrine (mcg/min)'
  when nip.unit_flag_new = 4 then 'Norepinephrine (mg/hr)'
  when nip.unit_flag_new = 5 then 'Norepinephrine (mg/min)'
  when nip.unit_flag_new = 6 then 'Norepinephrine (ml/hr)'
  when nip.unit_flag_new = 7 then 'Norepinephrine'
  else null end as drugname_new 
  from norepinephrine_in_part_3 nip  
)

, norepinephrine_in_part_5 as ( -- exist the units of Norepinephrine ()
  select nip.infusiondrugid
  , nip.patientunitstayid
  , nip.infusionoffset
  , nip.drugname_new as drugname
  , ifd.drugrate
  , ifd.infusionrate
  , ifd.drugamount
  , ifd.volumeoffluid
  , ifd.patientweight
  from norepinephrine_in_part_4 nip
  inner join infusiondrug_new_0 ifd 
  on nip.infusiondrugid = ifd.infusiondrugid
)

, norepinephrine_in_part_6 as (
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
  where ifd.drugname like '%Norepinephrine%'
  and ifd.patientunitstayid not in (select * from norepinephrine_in_part_0)
)

, norepinephrine_in_part as (
	select distinct *
	from (
		select *
		from norepinephrine_in_part_5
		union all
		select *
		from norepinephrine_in_part_6
	)
)

-- 2. Unified unit to mcg/kg/min
, norepinephrine_1 as (
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
    'Norepinephrine MAX 32 mg Dextrose 5% 250 ml (mcg/min)'      -- | 2585
    , 'Norepinephrine STD 4 mg Dextrose 5% 250 ml (mcg/min)'     -- | 3013
    , 'Norepinephrine STD 32 mg Dextrose 5% 282 ml (mcg/min)'    -- |  4
    , 'Norepinephrine STD 32 mg Dextrose 5% 500 ml (mcg/min)'  -- |  5
    , 'Norepinephrine STD 8 mg Dextrose 5% 250 ml (mcg/min)'   -- |  8
    , 'Norepinephrine STD 4 mg Dextrose 5% 500 ml (mcg/min)'   -- |  8
    , 'Norepinephrine STD 8 mg Dextrose 5% 500 ml (mcg/min)'   -- |  14
    , 'Norepinephrine MAX 32 mg Dextrose 5% 500 ml (mcg/min)'  -- |  15
  )
    then drugrate/(coalesce(coalesce(wi.admissionweight, wi.dischargeweight),80)) -- median(admissionweight) = 80
  when idn.drugname = 'Norepinephrine (mcg/kg/min)'    -- |  68921                        
    then drugrate
  when idn.drugname = 'Norepinephrine (mcg/min)'  --  |  192059
    then drugrate/(coalesce(coalesce(wi.admissionweight, wi.dischargeweight),80))
  when idn.drugname = 'Norepinephrine (mcg/hr)'  --  |   21
    then drugrate/(60 * coalesce(coalesce(wi.admissionweight, wi.dischargeweight),80))
  when idn.drugname = 'Norepinephrine (mg/hr)'  --  |   57
    then 1000*drugrate/(60 * coalesce(coalesce(wi.admissionweight, wi.dischargeweight),80))
  when idn.drugname = 'Norepinephrine (mcg/kg/hr)'  --  |  58
    then drugrate/60
  when idn.drugname = 'Norepinephrine (mg/min)'    --  |   8
    then 1000*drugrate/(coalesce(coalesce(wi.admissionweight, wi.dischargeweight),80))
  when idn.drugname = 'Norepinephrine (mg/kg/min)'  --  |    9
    then 1000*drugrate
  else null end as rate_norepinephrine  
  from norepinephrine_in_part idn
  left join `db_name.weight_icustay_detail_modify_eicu` wi 
  on idn.patientunitstayid = wi.patientunitstayid
  where drugname in (
    'Norepinephrine MAX 32 mg Dextrose 5% 250 ml (mcg/min)'      -- | 2585 
    , 'Norepinephrine STD 4 mg Dextrose 5% 250 ml (mcg/min)'     -- | 3013
    , 'Norepinephrine STD 32 mg Dextrose 5% 282 ml (mcg/min)'    -- |  4
    , 'Norepinephrine STD 32 mg Dextrose 5% 500 ml (mcg/min)'  -- |  5
    , 'Norepinephrine STD 8 mg Dextrose 5% 250 ml (mcg/min)'   -- |  8
    , 'Norepinephrine STD 4 mg Dextrose 5% 500 ml (mcg/min)'   -- |  8
    , 'Norepinephrine STD 8 mg Dextrose 5% 500 ml (mcg/min)'   -- |  14
    , 'Norepinephrine MAX 32 mg Dextrose 5% 500 ml (mcg/min)'  -- |  15
    , 'Norepinephrine (mcg/kg/min)'    -- |  68921  
    , 'Norepinephrine (mcg/min)'  --  |  192059  -- 192311
    , 'Norepinephrine (mcg/hr)'  --  |   21
    , 'Norepinephrine (mg/hr)'  --  |   57
    , 'Norepinephrine (mcg/kg/hr)'  --  |  58
    , 'Norepinephrine (mg/min)'    --  |   8 -- 9
    , 'Norepinephrine (mg/kg/min)'  --  |    9
    )
)

-- without consider : Norepinephrine (units/min) | 2; Norepinephrine | 1654 -- 8805
--                    norepinephrine Volume (ml) | 95; 

, norepinephrine_2 as (
  select idn.infusiondrugid
  , idn.patientunitstayid
  , idn.infusionoffset
  , idn.drugname
  , idn.infusionrate
  , idn.drugamount
  , idn.volumeoffluid
  , idn.patientweight   
  , case
  when md.drugname in (
    'PRMX NOREPInephrine 4 mg/250 mL NS Drip' -- |  1159
    , 'NOREPINEPHRINE 4 MG/250 ML NS'  -- |  2369
  )
  or md.drugname like 'NOREPINEPHRINE%4%MG%250%C%REPACKAGE%'  -- | 1647
    then 1000*idn.drugrate*4/(250*60*coalesce(coalesce(wi.admissionweight, wi.dischargeweight),80))
  when md.drugname in (
    '250 ML PLAS CONT : NOREPINEPHRINE IN D5W 8 MG/250 ML'  -- |   1171
    , 'NOREPINEPHRINE 8 MG in 250mL NS' --  |   5569  
  )
    then 1000*idn.drugrate*8/(250*60*coalesce(coalesce(wi.admissionweight, wi.dischargeweight),80))
  when md.drugname in (
    'NOREPINEPHRINE BITARTRATE 1 MG/ML AMP'        -- |  1225
    , 'NOREPINEPHRINE BITARTRATE 1 MG/ML IV : 4 ML'  -- |  1335
    , 'NOREPINEPHRINE BITARTRATE 1 MG/ML IV SOLN'    -- |  1919
    , 'NOREPINEPHRINE BITARTRATE 1 MG/ML IJ SOLN'    -- |  2137
  )  
    then 1000*idn.drugrate/(60*coalesce(coalesce(wi.admissionweight, wi.dischargeweight),80))
  when md.drugname = '<<norepinephrine 4 mg/4 mL Inj'   --   |   1736 
    then 0.2 -- clinican thinks this way and ml/hr, patients must be very serious, the part of sofa should be set 4 
  when md.drugname = 'NOREPINEPHRINE'   --  |    2089
    and md.dosage = '16 MG'
    then 1000*idn.drugrate*16/(250*60*coalesce(coalesce(wi.admissionweight, wi.dischargeweight),80))  -- set mL available : 250ml, maybe not right(just look many ml are 250ml)
  when md.drugname = 'NOREPINEPHRINE'   --  |    2089
    and md.dosage = '4 MG'
    then 1000*idn.drugrate*4/(250*60*coalesce(coalesce(wi.admissionweight, wi.dischargeweight),80))  -- set mL available : 250ml, maybe not right
  when md.drugname = 'NOREPINEPHRINE'   --  |    2089
    and md.dosage = '8 MG'
    then 1000*idn.drugrate*8/(250*60*coalesce(coalesce(wi.admissionweight, wi.dischargeweight),80))  -- set mL available : 250ml, maybe not right
  else null end as rate_norepinephrine

  from norepinephrine_in_part idn
  inner join `physionet-data.eicu_crd.medication` md
  on idn.patientunitstayid = md.patientunitstayid
  and md.drugordercancelled = 'No'
  and md.drugname like '%Norepinephrine%'
  left join `db_name.weight_icustay_detail_modify_eicu` wi 
  on idn.patientunitstayid = wi.patientunitstayid 
  where idn.drugname in (
    'norepinephrine Volume (ml) (ml/hr)'  -- | 13636 
    , 'Norepinephrine (ml/hr)'  -- |  272157  -- 272214
  )
  and idn.infusionoffset >= md.drugstartoffset
  and idn.infusionoffset <= md.drugstopoffset
)

, norepinephrine as (
    select distinct *
    from (
        select *
        from norepinephrine_1
        union all
        select *
        from norepinephrine_2
    )
)

select infusiondrugid
, patientunitstayid
, infusionoffset
, drugname
, round(rate_norepinephrine,4) as rate_norepinephrine
, infusionrate
, drugamount
, volumeoffluid
, patientweight 
from norepinephrine
order by patientunitstayid, infusionoffset;