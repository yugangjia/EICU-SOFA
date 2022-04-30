drop table if exists `db_name.pivoted_blood_pressure_eicu`;
create table `db_name.pivoted_blood_pressure_eicu` as
-- Here are three condition : if existing invasive measuring, will use them at first
-- 1) coalesce(invasive, non-invasive)
-- 2) coalesce(invasive vitalperiod, invasive nursing chart)
-- 3) coalesce(non-invasive vitalaperiod, non-invasive nursing chart)

with nurse_n_bp_0 as (
	select patientunitstayid
	, chartoffset as observationoffset
	, nibp_systolic as nsbp
	, nibp_diastolic as ndbp
	, round(COALESCE(nibp_mean, (nibp_diastolic + (nibp_systolic - nibp_diastolic)/3)),0) as mbp
	, (COALESCE(nibp_systolic,0) + COALESCE(nibp_diastolic,0) + COALESCE(nibp_mean,0))as del_flag
	from `physionet-data.eicu_crd_derived.pivoted_vital`
)

, nurse_n_bp as (
	select patientunitstayid
	, observationoffset
	, nsbp
	, ndbp
	, mbp
	, 4 as obsource
	from nurse_n_bp_0
	where del_flag != 0
)

, nurse_bp_0 as (
	select patientunitstayid
	, chartoffset as observationoffset
	, ibp_systolic as sbp
	, ibp_diastolic as dbp
	, round(COALESCE(ibp_mean, (ibp_diastolic + (ibp_systolic - ibp_diastolic)/3)),0) as map
	, (COALESCE(ibp_systolic,0) + COALESCE(ibp_diastolic,0) + COALESCE(ibp_mean,0))as del_flag
	from `physionet-data.eicu_crd_derived.pivoted_vital` 
)

, nurse_bp as (
	select patientunitstayid
	, observationoffset
	, sbp
	, dbp
	, map
	, 2 as obsource 
	from nurse_bp_0
	where del_flag != 0
)

, vitala_bp_0 as (
	select patientunitstayid
	, observationoffset
	, noninvasivesystolic as nsbp
	, noninvasivediastolic as ndbp
	, round(COALESCE(noninvasivemean, (noninvasivediastolic + (noninvasivesystolic - noninvasivediastolic)/3)),0) as mbp
	, (COALESCE(noninvasivesystolic,0) + COALESCE(noninvasivesystolic,0) + COALESCE(noninvasivemean,0))as del_flag
	from `physionet-data.eicu_crd.vitalaperiodic`
)

, vitala_bp as (
	select patientunitstayid
	, observationoffset
	, nsbp
	, ndbp
	, mbp
	, 3 as obsource
	from vitala_bp_0
	where del_flag != 0
)

, vital_bp_0 as (
	select patientunitstayid
	, observationoffset
	, systemicsystolic as sbp
	, systemicdiastolic as dbp
	, round(COALESCE(systemicmean, (systemicdiastolic + (systemicsystolic - systemicdiastolic)/3)),0) as map
	, (COALESCE(systemicsystolic,0) + COALESCE(systemicdiastolic,0) + COALESCE(systemicmean,0))as del_flag
	from `physionet-data.eicu_crd.vitalperiodic` 
)

, vital_bp as (
	select patientunitstayid
	, observationoffset
	, sbp
	, dbp
	, map
	, 1 as obsource
	from vital_bp_0
	where del_flag != 0
)

, bp_info_0 as (
	select patientunitstayid, observationoffset
	, nsbp as sbp
	, ndbp as dbp
	, mbp as map
	, obsource
	from nurse_n_bp
	union all
	select *
	from nurse_bp
	union all
	select patientunitstayid, observationoffset
	, nsbp as sbp
	, ndbp as dbp
	, mbp as map
	, obsource
	from vitala_bp
	union all
	select *
	from vital_bp
)

, sbp_info as (
	select patientunitstayid, observationoffset, sbp
	, ROW_NUMBER()
	OVER (partition by patientunitstayid, observationoffset order by obsource) as rn
	from bp_info_0
	where sbp > 0 and sbp < 400
)

, dbp_info as (
	select patientunitstayid, observationoffset, dbp
	, ROW_NUMBER()
	OVER (partition by patientunitstayid, observationoffset order by obsource) as rn
	from bp_info_0
	where dbp > 0 and dbp < 300
)

, map_info as (
	select patientunitstayid, observationoffset, map
	, ROW_NUMBER()
	OVER (partition by patientunitstayid, observationoffset order by obsource) as rn
	from bp_info_0
	where map > 0 and map < 300
)

, bp_time_info as (
	select patientunitstayid, observationoffset
	from bp_info_0
	group by patientunitstayid, observationoffset
)

select bi.patientunitstayid
, bi.observationoffset
, si.sbp
, di.dbp
, mi.map
from bp_time_info bi
left join sbp_info si 
on si.patientunitstayid = bi.patientunitstayid
and si.observationoffset = bi.observationoffset
and si.rn= 1
left join dbp_info di 
on di.patientunitstayid = bi.patientunitstayid
and di.observationoffset = bi.observationoffset
and di.rn = 1
left join map_info mi 
on mi.patientunitstayid = bi.patientunitstayid
and mi.observationoffset = bi.observationoffset
and mi.rn= 1
order by bi.patientunitstayid
, bi.observationoffset;
