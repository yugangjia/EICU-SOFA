# EICU-SOFA
The SQL query of calcualting hourly sofa score in eICU database: https://eicu-crd.mit.edu/, originally from Xiaoli Liu.

### Quickstart
First of all, create a new dataset inside your project.

![image](https://user-images.githubusercontent.com/23124524/167982493-afffe1df-a619-4cd7-a00f-1ab389bd651d.png)

Select your dataset name.

![image](https://user-images.githubusercontent.com/23124524/167982373-baf65570-b7ba-444e-8a02-627d4d6fdd61.png)

Change "db_name" inside Xiaoli's code into your unique dataset name. For example:

![image](https://user-images.githubusercontent.com/23124524/167982604-36427156-b833-43cc-8305-f80cfcba7911.png)

```
drop table if exists `db_name.weight_icustay_detail_modify_eicu`;
create table `db_name.weight_icustay_detail_modify_eicu` as

drop table if exists `alert-basis-349808.eICUsofa.weight_icustay_detail_modify_eicu`;
create table `alert-basis-349808.eICUsofa.weight_icustay_detail_modify_eicu` as
```
Run "weight_icustay_detail_modify_eicu.sql" first, next the other six queries other than "pivoted_sofa_eicu.sql". And finally "pivoted_sofa_eicu.sql".
