DECLARE
BEGIN
  EXECUTE IMMEDIATE('DROP TABLE AP_COLL.ilham_PAIDTILL_31');
  EXCEPTION WHEN OTHERS THEN NULL;
END;
/
CREATE TABLE AP_COLL.ilham_PAIDTILL_31 AS
  select distinct
        TYPE
      , DATE_TO
      , to_char(AREA_POS)AREA_POS 
      , name_goods_category
      , SELLER_TYPE
      , product_profile
      , product_type
      , Region_Area
      , inst_num
      , pre_strategy
      , contract_year_month
      , contract_year      
      , DPDT30
      , COUNT(DPDT30)over (partition by x.type, x.date_to, AREA_POS, name_goods_category, SELLER_TYPE, product_profile, product_type ,contract_year_month, contract_year, inst_num, pre_strategy, DPDT30) C_DPDT30
      , to_number(CNT)CNT
      , sum(dpd_0)over (partition by x.type, x.date_to, AREA_POS, name_goods_category, SELLER_TYPE, product_profile, product_type ,contract_year_month, contract_year, inst_num, pre_strategy, DPDT30) DPD_0
      , sum(dpd_5)over (partition by x.type, x.date_to, AREA_POS, name_goods_category, SELLER_TYPE, product_profile, product_type ,contract_year_month, contract_year, inst_num, pre_strategy, DPDT30) DPD_5
      , sum(dpd_10)over(partition by x.type, x.date_to, AREA_POS, name_goods_category, SELLER_TYPE, product_profile, product_type ,contract_year_month, contract_year, inst_num, pre_strategy, DPDT30) DPD_10
      , sum(dpd_20)over(partition by x.type, x.date_to, AREA_POS, name_goods_category, SELLER_TYPE, product_profile, product_type ,contract_year_month, contract_year, inst_num, pre_strategy, DPDT30) DPD_20
      , sum(dpd_30)over(partition by x.type, x.date_to, AREA_POS, name_goods_category, SELLER_TYPE, product_profile, product_type ,contract_year_month, contract_year, inst_num, pre_strategy, DPDT30) DPD_30
      , sum(dpd_60)over(partition by x.type, x.date_to, AREA_POS, name_goods_category, SELLER_TYPE, product_profile, product_type ,contract_year_month, contract_year, inst_num, pre_strategy, DPDT30) DPD_60
      , sum(dpd_90)over(partition by x.type, x.date_to, AREA_POS, name_goods_category, SELLER_TYPE, product_profile, product_type ,contract_year_month, contract_year, inst_num, pre_strategy, DPDT30) DPD_90
    from
     (
      select
        tim.type, tim.date_to
       , sum(1)over(partition by
                                  tim.type
                                , tim.date_to
                                , case when lower(AREA_POS) like '%closed%' then 'JABODETABEK' else AREA_POS end
                                , reg.name_goods_category
                                , reg.SELLER_TYPE 
                                , reg.product_profile
                                , reg.product_type 
                                , reg.CONTRACT_DECISIONYM
                                , reg.CONTRACT_DECISIONY
                                , ad.num_instalment_number 
                                ,case 
                                          when tim.date_to < to_date('2017-04-18', 'yyyy-mm-dd') then 'old'
           when tim.date_to between to_date('2017-04-18', 'yyyy-mm-dd') and to_date('2017-06-06', 'yyyy-mm-dd')  and to_number(substr(to_char(reg.text_contract_number),-3,3)) < 300 then 'old'
           when tim.date_to between to_date('2017-04-18', 'yyyy-mm-dd') and to_date('2017-06-06', 'yyyy-mm-dd')  and to_number(substr(to_char(reg.text_contract_number),-3,3)) > 300 then 'new'
           when tim.date_to between  to_date('2017-06-06', 'yyyy-mm-dd') and to_date('2017-12-31', 'yyyy-mm-dd')  and ad.num_instalment_number <= 2 then 'old'
           when tim.date_to between  to_date('2017-06-06', 'yyyy-mm-dd') and to_date('2017-12-31', 'yyyy-mm-dd')  and to_number(substr(to_char(reg.text_contract_number),-3,3)) < 700  then 'old'
           when tim.date_to between  to_date('2017-06-06', 'yyyy-mm-dd') and to_date('2017-12-31', 'yyyy-mm-dd')  and to_number(substr(to_char(reg.text_contract_number),-3,3)) >=700  then 'new'
           when (tim.date_to >= to_date('2018-01-01', 'yyyy-mm-dd')and ora_hash(text_contract_number,99,11302) < 70 and ad.num_instalment_number between 0 and 1 ) or (tim.date_to >= to_date('2018-01-01', 'yyyy-mm-dd') and ora_hash(text_contract_number,99,11302) < 40 and ad.num_instalment_number >=2 )
                                                 then 'new'/* including MPF always to High from -3 DPD */
                                                       else 'old' /* Hi Mi Lo Sl  */  
           /*else 'error'*/
                                  end
                    ) cnt
       , case when lower(AREA_POS) like '%closed%' then 'JABODETABEK' else AREA_POS end AREA_POS
       , case when lower(REGION_POS) like '%closed%' then 'Jakarta' else REGION_POS end REGION_POS
       , case when lower(SUBREGION_POS) like '%closed%' then 'Jakarta Selatan' else SUBREGION_POS end SUBREGION_POS
       , case when lower(DISTRICT_POS) like '%closed%' then 'Setiabudi' else DISTRICT_POS end DISTRICT_POS
       , reg.name_goods_category
       , reg.SELLER_TYPE
       , reg.product_profile
       , reg.product_type
       , NVL(ro.region_type,'-') as Region_Area
       , reg.CONTRACT_DECISIONYM contract_year_month
       , reg.CONTRACT_DECISIONY contract_year
       , ad.num_instalment_number AS inst_num
       , case 
           when tim.date_to < to_date('2017-04-18', 'yyyy-mm-dd') then 'old'
           when tim.date_to between to_date('2017-04-18', 'yyyy-mm-dd') and to_date('2017-06-06', 'yyyy-mm-dd')  and to_number(substr(to_char(reg.text_contract_number),-3,3)) < 300 then 'old'
           when tim.date_to between to_date('2017-04-18', 'yyyy-mm-dd') and to_date('2017-06-06', 'yyyy-mm-dd')  and to_number(substr(to_char(reg.text_contract_number),-3,3)) > 300 then 'new'
           when tim.date_to between  to_date('2017-06-06', 'yyyy-mm-dd') and to_date('2017-12-31', 'yyyy-mm-dd')  and ad.num_instalment_number <= 2 then 'old'
           when tim.date_to between  to_date('2017-06-06', 'yyyy-mm-dd') and to_date('2017-12-31', 'yyyy-mm-dd')  and to_number(substr(to_char(reg.text_contract_number),-3,3)) < 700  then 'old'
           when tim.date_to between  to_date('2017-06-06', 'yyyy-mm-dd') and to_date('2017-12-31', 'yyyy-mm-dd')  and to_number(substr(to_char(reg.text_contract_number),-3,3)) >=700  then 'new'
           when (tim.date_to >= to_date('2018-01-01', 'yyyy-mm-dd')and ora_hash(text_contract_number,99,11302) < 70 and ad.num_instalment_number between 0 and 1 ) or (tim.date_to >= to_date('2018-01-01', 'yyyy-mm-dd') and ora_hash(text_contract_number,99,11302) < 40 and ad.num_instalment_number >=2 )
                                                 then 'new'/* including MPF always to High from -3 DPD */
                                                       else 'old' /* Hi Mi Lo Sl  */  
           /*else 'error'*/
         end as pre_strategy 
       , case when tim.date_to > trunc(sysdate - 1) then null
         else case when cnt_days_overdue_tolerance_dd <= 0 then 1 else 0 end end dpd_0
       , case when tim.date_to + 5 > trunc(sysdate - 1) then null
         else case when cnt_days_overdue_tolerance_dd <= 5 and date_instalment < sysdate -5 then 1 else 0 end end dpd_5
       , case when tim.date_to + 10 > trunc(sysdate - 1) then null
         else case when cnt_days_overdue_tolerance_dd <= 10 and date_instalment < sysdate -10 then 1 else 0 end end dpd_10
       , case when tim.date_to + 20 > trunc(sysdate - 1) then null
         else case when cnt_days_overdue_tolerance_dd <= 20 and date_instalment < sysdate -20 then 1 else 0 end end dpd_20
       , case when tim.date_to + 30 > trunc(sysdate - 1) then null
         else case when cnt_days_overdue_tolerance_dd <= 30 and date_instalment < sysdate -30 then 1 else 0 end end dpd_30
       , case when tim.date_to + 60 > trunc(sysdate - 1) then null
         else case when cnt_days_overdue_tolerance_dd <= 60 and date_instalment < sysdate -60 then 1 else 0 end end dpd_60
       , case when tim.date_to + 90 > trunc(sysdate - 1) then null
         else case when cnt_days_overdue_tolerance_dd <= 90 and date_instalment < sysdate -90 then 1 else 0 end end dpd_90
           , case when cnt_days_overdue_tolerance_dd <= 31 then cnt_days_overdue_tolerance_dd else 999  end DPDt30
      FROM
                  owner_dwh.f_instalment_head_ad         ad
        left join ap_coll.vw_contract_region            reg ON  reg.skp_contract = ad.skp_contract
        left join AP_COLL.Sales_Region_Area             ro  ON  trim(upper(ro.name_sales_business_area)) = trim(upper(reg.name_sales_business_area))
        right join ap_coll.vw_report_timeline_paid_till tim ON  ad.date_instalment between tim.date_from and tim.date_to
                                                            and ((tim.type = 'day' and date_to >= last_day(add_months(trunc(sysdate-1),-3))+1)or (tim.type in ('week','month')))
      where tim.date_to >=TO_DATE('1.11.2017','DD.MM.YYYY')
      and ad.num_instalment_order is not null
      and ad.date_instalment < sysdate
      and ad.code_instalment_head_status = 'a'
      and ad.flag_deleted = 'N'
      AND ad.num_instalment_number < 100
    )x
; 
drop table ilham_paidtill_31b;
create table ilham_paidtill_31b as
SELECT 
        TYPE 
      , DATE_TO
      , AREA_POS 
      , name_goods_category
      , SELLER_TYPE
      , product_profile
      , product_type
      , Region_Area
      , inst_num
      , pre_strategy
      , contract_year_month
      , contract_year  ,    
      
      avg(cnt) as cnt
      FROM ilham_paidtill_31 
GROUP BY 

      TYPE 
      , DATE_TO
      , AREA_POS 
      , name_goods_category
      , SELLER_TYPE
      , product_profile
      , product_type
      , Region_Area
      , inst_num
      , pre_strategy
      , contract_year_month
      , contract_year      
      ;
      
      drop table ilham_paidtill_31c;
create table ilham_paidtill_31c as
select am.*
    from (select distinct b2.*, lvl dpd
            from ilham_paidtill_31b b2
            left join --Act as dpd for next step
           (select level - 1 lvl
             from (select 32 as x from dual)
           connect by level < = x
           union all
           select 611 lvl
             from dual
           union all
           select 999 lvl
             from dual)
              on 1 = 1) am;
              
              drop table ilham_paidtill_31e;
create table ilham_paidtill_31e as
select t.* , m.c_dpdt30, m.dpd_0, m.dpd_5, m.dpd_10, m.dpd_20,m.dpd_30 ,m.dpd_60,m.dpd_90   from ilham_paidtill_31c t
left join ilham_paidtill_31 m
on t.type = m.type
and t.date_to = m.date_to
and t.area_pos =m.area_pos
and t.name_goods_category = m.name_goods_category
and t.seller_type = m.seller_type
and t.product_profile = m.product_profile
and t.product_type = m.product_type
and t.region_area = m.region_area
and t.inst_num = m.inst_num
and t.pre_strategy = m.pre_strategy
and t.contract_year_month = m.contract_year_month
and t.contract_year = m.contract_year
and t.dpd = m.dpdt30;

drop table ilham_paidtill_31f;
 create table ilham_paidtill_31f as 
select 
        TYPE 
      , DATE_TO
      
      , SELLER_TYPE
      , product_profile
      , product_type
      
      , inst_num
      , pre_strategy
      , contract_year_month
      , contract_year      
      , dpd as DPDT30,
      SUM(C_DPDT30) as c_dpd3t30,
      sum(cnt) as cnt,
      SUM(DPD_0) as dpd_0,
      SUM(DPD_5) as dpd_5,
      SUM(DPD_10) as dpd_10,
      SUM(DPD_20) as dpd_20,
      SUM(DPD_30)as dpd_30,
      SUM(DPD_60) as dpd_60,
      SUM(DPD_90) as dpd_90
 from ilham_paidtill_31e 
 group by 
 TYPE 
      , DATE_TO
      
      , SELLER_TYPE
      , product_profile
      , product_type
      
      , inst_num
      , pre_strategy
      , contract_year_month
      , contract_year      
      , dpd 
       order by DATE_TO
      
      , SELLER_TYPE
      , product_profile
      , product_type
      
      , inst_num
      , pre_strategy
      , contract_year_month
      , contract_year      
      , DPDT30;
 drop table ilham_paidtill_31f2;     
 create table ilham_paidtill_31f2 as 
select 
       t.* , 
       sum(c_DPD3T30) over (partition by 
       TYPE 
      , DATE_TO
      
      , SELLER_TYPE
      , product_profile
      , product_type
      
      , inst_num
      , pre_strategy
      , contract_year_month
      , contract_year  order by DPDT30) as acc_dpd
 from ilham_paidtill_31f t;
 
 drop table ilham_paidtill_31f3; 
 create table  ilham_PAIDTILL_31f3 as 
with a1 as (
select t.type,t.date_to,t.seller_type,t.product_profile,t.product_type,t.inst_num,t.pre_strategy,t.contract_year_month,t.contract_year, avg(t.cnt),sum(t.dpd_0),sum(t.dpd_5),sum(t.dpd_10),sum(t.dpd_20),sum(t.dpd_30),sum(t.dpd_60),sum(t.dpd_90)  from ilham_PAIDTILL_31f2 t group by 
(t.type,t.date_to,t.seller_type,t.product_profile,t.product_type,t.inst_num,t.pre_strategy,t.contract_year_month,t.contract_year)),
a2 as(
SELECT t.*,
case when ((last_day(date_to))-(date_to)+1)=dpdt30 then 1 else 0 end as check1  FROM ilham_PAIDTILL_31f2 T) ,
a3 as (
SELECT * FROM a2 where check1 = 1),
a4  as  (
select a1.*,acc_dpd from a1 left join a3 
on a1.type = a3.type
and a1.date_to= a3.date_to
and a1.seller_type = a3.seller_type
and a1.product_profile =a3.product_profile
and a1.product_type = a3.product_type
and a1.inst_num= a3.inst_num
and a1.pre_strategy = a3.pre_strategy
and a1.contract_year_month = a3.contract_year_month
and a1.contract_year = a3.contract_year)
  
  SELECT * FROM a4 ;
