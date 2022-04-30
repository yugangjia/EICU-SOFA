drop table if exists `db_name.pivoted_vent_part1_eicu`;
create table `db_name.pivoted_vent_part1_eicu` as

-- There are three tables (careplangeneral, respiratorycare, respiratorycharting) existing ventaliation information
-- While the info of respiratorycare is not accurate. So, we will get the detail from careplangeneral table

with ventaliation_info as (
	select cplgeneralid
	, patientunitstayid
	, activeupondischarge
	, cplitemoffset
	, cplgroup
	, cplitemvalue
	, case 
	when cplitemvalue like 'Intubated%' 
	or cplitemvalue = 'Ventilated - chronic dependency'
	or cplitemvalue = 'Ventilated - with daily extubation evaluation'
	or cplitemvalue = 'Ventilated - with no daily extubation trial'
	or cplitemvalue = 'Non-invasive ventilation' then 1
    else 0 end as vent_flag
	-- Intubated/nasal ETT	            | 335
	-- Intubated/nasal ETT - difficult	| 52
	-- Intubated/oral ETT	            | 59566
	-- Intubated/oral ETT - difficult	| 798
	-- Intubated/trach-acute	        | 4829
	-- Intubated/trach-chronic	        | 4993
	-- Ventilated - chronic dependency	                | 3105
	-- Ventilated - with daily extubation evaluation	| 51862
 	-- Ventilated - with no daily extubation trial	    | 14907  
    -- Non-invasive ventilation	                        | 26836

    -- Ventilated - rapid wean/extubation	      | 5705
	-- Not intubated/normal airway	              | 206795
	-- Not intubated/partial airway obstruction	  | 1543
	-- Spontaneous - adequate	                  | 190809
	-- Spontaneous - tenuous	                  | 32587
	--                                            | 14896	
	from `physionet-data.eicu_crd.careplangeneral`
	where cplgroup in ('Airway', 'Ventilation') 
	and cplitemvalue != ''
)

, ventilation_00 as (
	select patientunitstayid
	, sum(vent_flag) as num
	from ventaliation_info
	group by patientunitstayid
)

, ventilation_01 as ( -- drop patientunitstayid didn't have ventaliation
	select patientunitstayid
	, cplitemoffset
	, sum(vent_flag) as num
	from ventaliation_info
	where patientunitstayid not in (
		select patientunitstayid 
		from ventilation_00 
		where num = 0 
		group by patientunitstayid
		)
	group by patientunitstayid
	, cplitemoffset
)

, ventilation_02 as (
	select vi.cplgeneralid
	, vi.patientunitstayid
	, vi.activeupondischarge
	, vi.cplitemoffset
	, vi.cplgroup
	, vi.cplitemvalue
	, vi.vent_flag
	, ROW_NUMBER()
	over (partition by vi.patientunitstayid, vi.cplitemoffset order by vi.vent_flag desc) as flag
	from ventaliation_info vi 
	inner join ventilation_01 v0
	on vi.patientunitstayid = v0.patientunitstayid
	and vi.cplitemoffset = v0.cplitemoffset
	where v0.num >= 1
	and vi.vent_flag = 0
)

-- drop the same cplitemoffset rows of non-ventiliation, existing ventiliation and non-ventiliation
, ventilation_0 as ( 
	select vi.cplgeneralid
	, vi.patientunitstayid
	, vi.activeupondischarge
	, vi.cplitemoffset
	, vi.cplgroup
	, vi.cplitemvalue
	, vi.vent_flag
	from ventaliation_info vi
	where vi.cplgeneralid not in (select cplgeneralid from ventilation_02 where flag = 1)
)

-- solving the same cplitemoffset rows of more than two different ventiliation
-- remain one rows
, ventilation_10 as (
	select cplgeneralid
	, ROW_NUMBER() 
	OVER (partition by patientunitstayid, cplitemoffset order by cplitemvalue) as rn
	from ventilation_0
	where vent_flag = 1
)

select *
from ventilation_0
where cplgeneralid not in (select cplgeneralid from ventilation_10 where rn > 1)
order by patientunitstayid, cplitemoffset;



drop table if exists `db_name.pivoted_vent_part2_eicu`;
create table `db_name.pivoted_vent_part2_eicu` as

-- if existing: delete the first rows of non-ventiliation
with ventilation_20 as (
	select *
	, ROW_NUMBER()
	OVER (partition by patientunitstayid order by cplitemoffset) as rn
	from `db_name.pivoted_vent_part1_eicu`
)

, ventilation_21 as (
	select *
	from ventilation_20
	where patientunitstayid in (
		select patientunitstayid
		from ventilation_20
		where rn = 1 and vent_flag = 0
		group by patientunitstayid
	)
)

, ventilation_22 as (
	select *
	from ventilation_20
	where cplgeneralid not in (select cplgeneralid from ventilation_21)
)

-- ventilation_21: delete the first rows of non-ventiliation
, ventilation_210 as (
	select cplgeneralid
	, patientunitstayid
	, activeupondischarge
	, cplitemoffset
	, cplgroup
	, cplitemvalue
	, vent_flag
	, LAG(vent_flag, 1) OVER (partition by patientunitstayid order by cplitemoffset) as vent_flag_new
	from ventilation_21
	-- order by patientunitstayid
	-- , cplitemoffset
)

, ventilation_211 as (
	select *
	, vent_flag_new - vent_flag as del_flag
	, ROW_NUMBER()
	OVER (partition by patientunitstayid order by cplitemoffset) as rn
	from ventilation_210
	where vent_flag_new - vent_flag = -1
)

, ventilation_212 as (
	select *
	from ventilation_211
	where rn = 1
)

, ventilation_213 as (
	select v21.*
	from ventilation_21 v21
	inner join ventilation_212 v212
	on v21.patientunitstayid = v212.patientunitstayid
	and v21.cplitemoffset >= v212.cplitemoffset
)

, ventilation_2 as (
	select cplgeneralid
	, patientunitstayid
	, activeupondischarge
	, cplitemoffset
	, cplgroup
	, cplitemvalue
	, vent_flag
	from ventilation_213
	union all
	select cplgeneralid
	, patientunitstayid
	, activeupondischarge
	, cplitemoffset
	, cplgroup
	, cplitemvalue
	, vent_flag
	from ventilation_22 
)

select distinct *
from ventilation_2
order by patientunitstayid, cplitemoffset;



drop table if exists `db_name.pivoted_vent_part34_eicu`;
create table `db_name.pivoted_vent_part34_eicu` as

-- delete the same cplitemoffset with different types of non-ventilation, remain one row
with ventilation_30 as (
	select cplgeneralid
	, patientunitstayid
	, activeupondischarge
	, cplitemoffset
	, cplgroup
	, cplitemvalue
	, vent_flag
	, ROW_NUMBER()
	over (partition by patientunitstayid, cplitemoffset order by cplitemvalue desc) as rn
	from `db_name.pivoted_vent_part2_eicu`
	where vent_flag = 0
)

, ventilation_31 as (
	select cplgeneralid
	, patientunitstayid
	, activeupondischarge
	, cplitemoffset
	, cplgroup
	, cplitemvalue
	, vent_flag
	from `db_name.pivoted_vent_part2_eicu`
	where vent_flag != 0
)

, ventilation_3 as (
	select cplgeneralid
	, patientunitstayid
	, activeupondischarge
	, cplitemoffset
	, cplgroup
	, cplitemvalue
	, vent_flag
	from ventilation_30
	where rn = 1
	union all
	select cplgeneralid
	, patientunitstayid
	, activeupondischarge
	, cplitemoffset
	, cplgroup
	, cplitemvalue
	, vent_flag
	from ventilation_31
)

-- existing some patientunitstayid didn't know the endtime
-- Assume that it ends after 1h
, ventilation_40 as (
	select *
	, ROW_NUMBER()
	OVER (partition by patientunitstayid order by cplitemoffset desc) as rn
	from (select distinct * from ventilation_3)
)

, ventilation_41 as (
	select cplgeneralid
	, patientunitstayid
	, activeupondischarge
	, cplitemoffset
	, cplgroup
	, cplitemvalue
	, vent_flag 
	from ventilation_40
	where rn = 1
	and vent_flag = 1
	and activeupondischarge is false
)

, ventilation_411 as (
    select cplgeneralid
	, patientunitstayid
	, activeupondischarge
	, cplitemoffset
	, cplgroup
	, cplitemvalue
	, vent_flag
	from ventilation_41
	union all
	select cplgeneralid
	, patientunitstayid
	, activeupondischarge
	, cplitemoffset + 60 as cplitemoffset
	, cplgroup
	, 'Spontaneous - adequate' as cplitemvalue
	, 0 as vent_flag
	from ventilation_41  
)

, ventilation_42 as (
	select cplgeneralid
	, patientunitstayid
	, activeupondischarge
	, cplitemoffset
	, cplgroup
	, cplitemvalue
	, vent_flag 
	from ventilation_40
	where cplgeneralid not in (select cplgeneralid from ventilation_41)
)

, ventilation_4 as (
	select *
	from ventilation_411
	union all
	select *
	from ventilation_42
)

select distinct *
from ventilation_4
order by patientunitstayid, cplitemoffset;



drop table if exists `db_name.pivoted_vent_part56_eicu`;
create table `db_name.pivoted_vent_part56_eicu` as

-- existing some patients : the last two rows were active ventilation and active non-ventilation
-- we will handle this situation : assume that patient finish ventilation before start non-ventilation
with ventilation_50 as (
	select *
	, ROW_NUMBER()
	OVER (partition by patientunitstayid order by cplitemoffset desc) as rn
	from `db_name.pivoted_vent_part34_eicu`
)

, ventilation_500 as (
	select *
	, case
	when rn = 1 and vent_flag = 0 then 1
	else 0 end as flag
	from ventilation_50
)

, ventilation_501 as (
	select *
	, case
	when rn = 2 and vent_flag = 1 and activeupondischarge is true then 1
	else 0 end as flag
	from ventilation_50
)

, ventilation_502 as (
	select patientunitstayid
	from ventilation_500
	where patientunitstayid in (
		select patientunitstayid 
		from ventilation_501 
		where flag=1 
		group by patientunitstayid
		)
	and flag = 1
	group by patientunitstayid
)

, ventilation_510 as ( --  needing to modify
	select cplgeneralid
	, patientunitstayid
	, activeupondischarge
	, cplitemoffset
	, cplgroup
	, cplitemvalue
	, vent_flag
	, ROW_NUMBER()
	OVER (partition by patientunitstayid order by cplitemoffset desc) as rn
	from ventilation_50
	where patientunitstayid in (select * from ventilation_502)
)

, ventilation_51 as (
	select cplgeneralid
	, patientunitstayid
	, activeupondischarge
	, cplitemoffset
	, cplgroup
	, cplitemvalue
	, vent_flag	
	from ventilation_510
	where rn > 1
	union all
	select cplgeneralid
	, patientunitstayid
	, false as activeupondischarge
	, cplitemoffset
	, cplgroup
	, 'Spontaneous - adequate' as cplitemvalue
	, 0 as vent_flag	
	from ventilation_510
	where rn = 1	
)


, ventilation_52 as (
	select cplgeneralid
	, patientunitstayid
	, activeupondischarge
	, cplitemoffset
	, cplgroup
	, cplitemvalue
	, vent_flag
	from ventilation_50
	where patientunitstayid not in (select * from ventilation_502)
)

, ventilation_5 as (
	select distinct *
	from (
		select *
		from ventilation_51
		union all
		select *
		from ventilation_52
	)
)

-- handling with tha last row is activeupondischarge = True and vent_flag = 1
, ventilation_60 as (
	select *
	, ROW_NUMBER()
	OVER (partition by patientunitstayid order by cplitemoffset desc) as rn
	from ventilation_5
)

, ventilation_610 as (
	select cplgeneralid
	, patientunitstayid
	, activeupondischarge
	, cplitemoffset
	, cplgroup
	, cplitemvalue
	, vent_flag
	, rn
	from ventilation_60 
	where patientunitstayid in (
		select patientunitstayid
		from ventilation_60
		where rn = 1
		and vent_flag = 1
		and activeupondischarge is true
		group by patientunitstayid
		)
)

, ventilation_61 as (
	select cplgeneralid
	, patientunitstayid
	, activeupondischarge
	, cplitemoffset
	, cplgroup
	, cplitemvalue
	, vent_flag
	from ventilation_610
	union all
	select vt.cplgeneralid
	, vt.patientunitstayid
	, false as activeupondischarge
	, icud.unitdischargeoffset as cplitemoffset
	, 'Airway' as cplgroup
	, 'Spontaneous - adequate' as cplitemvalue
	, 0 as vent_flag
	from ventilation_610 vt
	left join `physionet-data.eicu_crd_derived.icustay_detail` icud 
	on vt.patientunitstayid = icud.patientunitstayid
	where vt.rn = 1
)

, ventilation_62 as (
	select cplgeneralid
	, patientunitstayid
	, activeupondischarge
	, cplitemoffset
	, cplgroup
	, cplitemvalue
	, vent_flag
	from ventilation_60
	where cplgeneralid not in (
		select cplgeneralid
		from ventilation_610
		)
)

, ventilation_6 as (
	select *
	from ventilation_61
	union all
	select *
	from ventilation_62
)

select distinct *
from ventilation_6
order by patientunitstayid, cplitemoffset;



drop table if exists `db_name.pivoted_vent_eicu`;
create table `db_name.pivoted_vent_eicu` as

-- get start and end time of ventilation
with ventilation_70 as (
	select patientunitstayid
	, activeupondischarge
	, cplitemoffset as starttime
	, lead(cplitemoffset, 1) OVER (partition by patientunitstayid order by cplitemoffset) as endtime
	, cplgroup
	, cplitemvalue
	, vent_flag
	, lead(vent_flag, 1) OVER (partition by patientunitstayid order by cplitemoffset) as vent_flag_new
	from `db_name.pivoted_vent_part56_eicu`
)

, ventilation_701 as (
	select *
	, vent_flag - vent_flag_new as flag
	from ventilation_70
	where vent_flag = 1
	and vent_flag - vent_flag_new != -1
)

, ventilation_71 as (
	select patientunitstayid
	, starttime
	, endtime
	from ventilation_701
	where flag = 1
)

, ventilation_720 as (
	select distinct *
	from (
		select patientunitstayid
		, starttime as cplitemoffset
		from ventilation_701
		where flag = 0
		union all
		select patientunitstayid
		, endtime as cplitemoffset
		from ventilation_701
		where flag = 0
	)
)

, ventilation_721 as (
	select patientunitstayid
	, cplitemoffset
	, count(cplitemoffset) as num
	from ventilation_720
	group by patientunitstayid
	, cplitemoffset
)

, ventilation_72 as (
	select patientunitstayid
	, cplitemoffset
	from ventilation_721
	where num = 1
)

, ventilation_730 as (
	select distinct *
	from (
		select patientunitstayid
		, starttime as cplitemoffset
		from ventilation_71
		union all
		select patientunitstayid
		, endtime as cplitemoffset
		from ventilation_71
		union all
		select patientunitstayid
		, cplitemoffset
		from ventilation_72
	)
)

, ventilation_731 as (
	select patientunitstayid
	, cplitemoffset
	, count(cplitemoffset) as num
	from ventilation_730
	group by patientunitstayid
	, cplitemoffset
)

, ventilation_732 as (
	select patientunitstayid
	, cplitemoffset
	from ventilation_731
	where num = 1
	order by cplitemoffset
)

, ventilation_733 as (
	select patientunitstayid
	, cplitemoffset as starttime
    , lead(cplitemoffset, 1) OVER (partition by patientunitstayid order by cplitemoffset) as endtime
    from ventilation_732
)

, ventilation_734 as (
	select *
	, ROW_NUMBER()
	OVER (partition by patientunitstayid order by starttime) as rn
	from ventilation_733
	where endtime is not null
)

select patientunitstayid
, starttime
, endtime
from ventilation_734
where mod(rn,2) = 1
order by patientunitstayid
, starttime;


-- drop temporal tables
drop table if exists `db_name.pivoted_vent_part1_eicu`;
drop table if exists `db_name.pivoted_vent_part2_eicu`;
drop table if exists `db_name.pivoted_vent_part34_eicu`;
drop table if exists `db_name.pivoted_vent_part56_eicu`;