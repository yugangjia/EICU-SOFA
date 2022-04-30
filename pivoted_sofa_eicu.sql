-- By Xiaoli Liu 
-- 2018.12.23 & 2019.11.08 &2021.09.06
-- https://github.com/MIT-LCP/mimic-code/blob/main/mimic-iii/concepts/pivot/pivoted_sofa.sql
-- using :
--         pivoted_bg.sql                     : PaO2/FiO2                                                |   (eicu_crd_derived)
--         pivoted_vent.sql                   : Mechanical Ventilation                                   |   (new-generation)
--         pivoted_score.sql                  : gcs                                                      |   (eicu_crd_derived)
--         pivoted_blood_pressure.sql         : map                                                      |   (new-generation)
--         norepinephrine_info.sql | dopamine_info.sql | epinephrine_info.sql | dobutamine_info.sql      |   (new-generation)
--         pivoted_lab.sql                    : bilirubin | platelet | creatinine                        |   (eicu_crd_derived)
--         pivoted_uo.sql                     : Urine Output                                             |   (eicu_crd_derived)
--         icustay_detail.sql                                                                            |   (eicu_crd_derived) 
--         weight_icustay_detail_modify.sql   : weight                                                   |   (new-generation)

drop table if exists `db_name.pivoted_sofa_eicu`;
create table `db_name.pivoted_sofa_eicu` as

with cohort_info_0 as (
  select icud.patientunitstayid
  , ceil(unitdischargeoffset/60) as los_icu_hours
  , case when age = '> 89' then '91.4'
  else age end as age
  from `physionet-data.eicu_crd_derived.icustay_detail` icud
  where age != ''
)

, cohort_info as (
  select patientunitstayid
  , los_icu_hours
  , cast(age as numeric) as age
  from cohort_info_0
)

, all_hours as
(
  select patientunitstayid
  , GENERATE_ARRAY(-24, CAST(ceil(los_icu_hours) AS INT64)) as hr
  from cohort_info
)

, co as (
  select patientunitstayid, hr
  from all_hours 
  cross join unnest(all_hours.hr) as hr
)


-- Part 1 PaO2/FiO2 + Mechanical Ventilation
-- Part 2 GCS
-- Part 3 MAP + Norepinephrine | Dopamine | Epinephrine | Dobutamine
-- Part 4 Bilirubin
-- Part 5 Platelet
-- Part 6 Creatinine + Urine Output 
-- get minimum blood pressure from chartevents
, bp as
(
  select pv.patientunitstayid
    , pv.observationoffset as chartoffset
    , min(map) as MeanBP_min
  from `db_name.pivoted_blood_pressure_eicu` pv
  -- exclude rows marked as error
  where map > 0 and map < 300
  group by pv.patientunitstayid
    , pv.observationoffset
)

, pafi_0 as (
  select patientunitstayid, chartoffset
  , round(pao2/fio2,0) as pao2fio2ratio
  from `physionet-data.eicu_crd_derived.pivoted_bg`
  where fio2 is not null
  and pao2 is not null
)

, pafi as
(
  -- join blood gas to ventilation durations to determine if patient was vent
  select pf.patientunitstayid
  , pf.chartoffset
  -- because pafi has an interaction between vent/PaO2:FiO2, we need two columns for the score
  -- it can happen that the lowest unventilated PaO2/FiO2 is 68, but the lowest ventilated PaO2/FiO2 is 120
  -- in this case, the SOFA score is 3, *not* 4.
  , case when pv.patientunitstayid is null then pao2fio2ratio else null end PaO2FiO2Ratio_novent
  , case when pv.patientunitstayid is not null then pao2fio2ratio else null end PaO2FiO2Ratio_vent
  from pafi_0 pf
  left join `db_name.pivoted_vent_eicu` pv
    on pf.patientunitstayid = pv.patientunitstayid
    and pf.chartoffset >= pv.starttime
    and pf.chartoffset <= pv.endtime
)

, uo as (
  select co.patientunitstayid, co.hr
  , sum(uo.urineoutput) as UrineOutput
  from  co
  left join `physionet-data.eicu_crd_derived.pivoted_uo` uo
    on co.patientunitstayid = uo.patientunitstayid
    and 60*(co.hr-1) < uo.chartoffset
    and 60*co.hr >= uo.chartoffset
  group by co.patientunitstayid, co.hr
)

, scorecomp_0 as
(
  select co.patientunitstayid, co.hr
  -- vitals
  , min(bp.MeanBP_min) as MeanBP_min
  -- gcs
  , min(ps.gcs) as GCS_min
  -- labs
  , max(labs.bilirubin) as bilirubin_max
  , max(labs.creatinine) as creatinine_max
  , min(labs.platelets) as platelet_min
  , min(pf.PaO2FiO2Ratio_novent) as PaO2FiO2Ratio_novent
  , min(pf.PaO2FiO2Ratio_vent) as PaO2FiO2Ratio_vent
  , max(ni.rate_norepinephrine) as rate_norepinephrine
  , max(ei.rate_epinephrine) as rate_epinephrine
  , max(di.rate_dobutamine) as rate_dobutamine
  , max(dop.rate_dopamine) as rate_dopamine
  from co
  left join bp
    on co.patientunitstayid = bp.patientunitstayid
    and 60*(co.hr-1) < bp.chartoffset
    and 60*co.hr >= bp.chartoffset
  left join `physionet-data.eicu_crd_derived.pivoted_score` ps
    on co.patientunitstayid = ps.patientunitstayid
    and 60*(co.hr-1) < ps.chartoffset
    and 60*co.hr >= ps.chartoffset
  left join `physionet-data.eicu_crd_derived.pivoted_lab` labs
    on co.patientunitstayid = labs.patientunitstayid
    and 60*(co.hr-1) < labs.chartoffset
    and 60*co.hr >= labs.chartoffset
  left join pafi pf
    on co.patientunitstayid = pf.patientunitstayid
    and 60*(co.hr-1) < pf.chartoffset
    and 60*co.hr >= pf.chartoffset
  left join `db_name.norepinephrine_info_eicu` ni 
    on co.patientunitstayid = ni.patientunitstayid
    and 60*(co.hr-1) < ni.infusionoffset
    and 60*co.hr >= ni.infusionoffset
    and rate_norepinephrine > 0
  left join `db_name.epinephrine_info_eicu` ei 
    on co.patientunitstayid = ei.patientunitstayid
    and 60*(co.hr-1) < ei.infusionoffset
    and 60*co.hr >= ei.infusionoffset
    and rate_epinephrine > 0
  left join `db_name.dobutamine_info_eicu` di 
    on co.patientunitstayid = di.patientunitstayid
    and 60*(co.hr-1) < di.infusionoffset
    and 60*co.hr >= di.infusionoffset
    and rate_dobutamine > 0
  left join `db_name.dopamine_info_eicu` dop 
    on co.patientunitstayid = dop.patientunitstayid
    and 60*(co.hr-1) < dop.infusionoffset
    and 60*co.hr >= dop.infusionoffset
    and rate_dopamine > 0  
  group by co.patientunitstayid, co.hr
)

, scorecomp as (
  select s.*
  -- uo
  , uo.UrineOutput as urineoutput
  from scorecomp_0 s 
  left join uo
    on s.patientunitstayid = uo.patientunitstayid
    and s.hr = uo.hr
)

, scorecalc as
(
  -- Calculate the final score
  -- note that if the underlying data is missing, the component is null
  -- eventually these are treated as 0 (normal), but knowing when data is missing is useful for debugging
  select scorecomp.*
  -- Respiration
  , case
      when PaO2FiO2Ratio_vent   < 100 then 4
      when PaO2FiO2Ratio_vent   < 200 then 3
      when PaO2FiO2Ratio_novent < 300 then 2
      when PaO2FiO2Ratio_novent < 400 then 1
      when coalesce(PaO2FiO2Ratio_vent, PaO2FiO2Ratio_novent) is null then null
      else 0
    end as respiration

  -- Coagulation
  , case
      when platelet_min < 20  then 4
      when platelet_min < 50  then 3
      when platelet_min < 100 then 2
      when platelet_min < 150 then 1
      when platelet_min is null then null
      else 0
    end as coagulation

  -- Liver
  , case
      -- Bilirubin checks in mg/dL
        when Bilirubin_Max >= 12.0 then 4
        when Bilirubin_Max >= 6.0  then 3
        when Bilirubin_Max >= 2.0  then 2
        when Bilirubin_Max >= 1.2  then 1
        when Bilirubin_Max is null then null
        else 0
      end as liver

  -- Cardiovascular
  , case
      when rate_dopamine > 15 or rate_epinephrine >  0.1 or rate_norepinephrine >  0.1 then 4
      when rate_dopamine >  5 or rate_epinephrine <= 0.1 or rate_norepinephrine <= 0.1 then 3
      when rate_dopamine >  0 or rate_dobutamine > 0 then 2
      when MeanBP_Min < 70 then 1
      when coalesce(MeanBP_Min, rate_dopamine, rate_dobutamine, rate_epinephrine, rate_norepinephrine) is null then null
      else 0
    end as cardiovascular

  -- Neurological failure (GCS)
  , case
      when (GCS_min >= 13 and GCS_min <= 14) then 1
      when (GCS_min >= 10 and GCS_min <= 12) then 2
      when (GCS_min >=  6 and GCS_min <=  9) then 3
      when  GCS_min <   6 then 4
      when  GCS_min is null then null
  else 0 end
    as cns

  -- Renal failure - high creatinine or low urine output
  , case
    when (Creatinine_Max >= 5.0) then 4
    when
      SUM(urineoutput) OVER (PARTITION BY patientunitstayid ORDER BY hr
      ROWS BETWEEN 23 PRECEDING AND 0 FOLLOWING) < 200
        then 4
    when (Creatinine_Max >= 3.5 and Creatinine_Max < 5.0) then 3
    when
      SUM(urineoutput) OVER (PARTITION BY patientunitstayid ORDER BY hr
      ROWS BETWEEN 23 PRECEDING AND 0 FOLLOWING) < 500
        then 3
    when (Creatinine_Max >= 2.0 and Creatinine_Max < 3.5) then 2
    when (Creatinine_Max >= 1.2 and Creatinine_Max < 2.0) then 1
    when coalesce
      (
        SUM(urineoutput) OVER (PARTITION BY patientunitstayid ORDER BY hr
        ROWS BETWEEN 23 PRECEDING AND 0 FOLLOWING)
        , Creatinine_Max
      ) is null then null
  else 0 end
    as renal
  from scorecomp
)

, score_final as
(
  select s.*
    -- Combine all the scores to get SOFA
    -- Impute 0 if the score is missing
   -- the window function takes the max over the last 24 hours
    , coalesce(
        MAX(respiration) OVER (PARTITION BY patientunitstayid ORDER BY HR
        ROWS BETWEEN 23 PRECEDING AND 0 FOLLOWING)
      ,0) as respiration_24hours
     , coalesce(
         MAX(coagulation) OVER (PARTITION BY patientunitstayid ORDER BY HR
         ROWS BETWEEN 23 PRECEDING AND 0 FOLLOWING)
        ,0) as coagulation_24hours
    , coalesce(
        MAX(liver) OVER (PARTITION BY patientunitstayid ORDER BY HR
        ROWS BETWEEN 23 PRECEDING AND 0 FOLLOWING)
      ,0) as liver_24hours
    , coalesce(
        MAX(cardiovascular) OVER (PARTITION BY patientunitstayid ORDER BY HR
        ROWS BETWEEN 23 PRECEDING AND 0 FOLLOWING)
      ,0) as cardiovascular_24hours
    , coalesce(
        MAX(cns) OVER (PARTITION BY patientunitstayid ORDER BY HR
        ROWS BETWEEN 23 PRECEDING AND 0 FOLLOWING)
      ,0) as cns_24hours
    , coalesce(
        MAX(renal) OVER (PARTITION BY patientunitstayid ORDER BY HR
        ROWS BETWEEN 23 PRECEDING AND 0 FOLLOWING)
      ,0) as renal_24hours

    -- sum together data for final SOFA
    , coalesce(
        MAX(respiration) OVER (PARTITION BY patientunitstayid ORDER BY HR
        ROWS BETWEEN 23 PRECEDING AND 0 FOLLOWING)
      ,0)
     + coalesce(
         MAX(coagulation) OVER (PARTITION BY patientunitstayid ORDER BY HR
         ROWS BETWEEN 23 PRECEDING AND 0 FOLLOWING)
      ,0)
     + coalesce(
        MAX(liver) OVER (PARTITION BY patientunitstayid ORDER BY HR
        ROWS BETWEEN 23 PRECEDING AND 0 FOLLOWING)
      ,0)
     + coalesce(
        MAX(cardiovascular) OVER (PARTITION BY patientunitstayid ORDER BY HR
        ROWS BETWEEN 23 PRECEDING AND 0 FOLLOWING)
      ,0)
     + coalesce(
        MAX(cns) OVER (PARTITION BY patientunitstayid ORDER BY HR
        ROWS BETWEEN 23 PRECEDING AND 0 FOLLOWING)
      ,0)
     + coalesce(
        MAX(renal) OVER (PARTITION BY patientunitstayid ORDER BY HR
        ROWS BETWEEN 23 PRECEDING AND 0 FOLLOWING)
      ,0)
    as SOFA_24hours
  from scorecalc s
)
select * from score_final
where hr >= 0
order by patientunitstayid, hr;