with cal as 
(
  select 
  distinct(date(week_start)) as reporting_week_start_date
  from se.data.se_calendar
  where date(week_start) > ('2018-12-05')
  )

, sign_up as 
(
  select
  shiro_user_id
  , cohort_year_month sign_up_month
  , date(signup_tstamp) sign_up_date
  , date(date_trunc(week, signup_tstamp)) sign_up_week
  , case when current_affiliate_territory	 in ('DE','CH') then 'DACH'   -- take terriroty from se.data_pii.se_user_attributes
         when current_affiliate_territory = 'UK' then 'UK'
         else 'ROW' end as territory
  from SE.DATA.SE_USER_ATTRIBUTES
)

// margin per booker lw 
, revenue_by_week_by_user as (
  select
  shiro_user_id
  , sign_up_week
  , territory
  , date_trunc(week, date(booking_completed_date))  as reporting_week
  , case when sign_up_week =  date_trunc(week, date(booking_completed_date))  //signed up in same week
                or sign_up_week =  dateadd(day, -7, date_trunc(week, date(booking_completed_date)))  //signed up in last week
         then 1 else 0 end as recent_sign_up_flag
  , count(distinct booking_id) as total_bookings
  , sum(margin_gross_of_toms_gbp_constant_currency)as  total_margin 
  from se.data.fact_booking  

  left join sign_up using(shiro_user_id)
  
  where  booking_status_type in ('live','cancelled')
  and date(booking_completed_date) > ('2018-12-05')
  GROUP BY 1,2,3,4,5
  )
  
//  select * from revenue_by_week_by_user where recent_sign_up_flag  = 1 //checks
  

// spvs per cust lw
, spvs_by_user_by_week as (
    select 
    attributed_user_id as shiro_user_id
    , sign_up_week
    , territory
    , date_trunc(week, date(stba.TOUCH_START_TSTAMP))  as reporting_week
  
    , case when sign_up_week =  date_trunc(week, date(stba.TOUCH_START_TSTAMP))  //signed up in same week
                or sign_up_week =  dateadd(day, -7, date_trunc(week,date(stba.TOUCH_START_TSTAMP)))  //signed up in last week
         then 1 else 0 end as recent_sign_up_flag
  
    ,count(distinct touch_id) as sessions 
    ,count(distinct event_hash) as spvs 
  
  from se.data_pii.scv_touch_basic_attributes stba 
  left join se.data.scv_touched_spvs sts using(touch_id)
  
  left join sign_up on shiro_user_id = attributed_user_id
  
  where stitched_identity_type = 'se_user_id'
  and date(stba.TOUCH_START_TSTAMP) > ('2018-12-05')
  
  group by 1,2,3,4,5
    ) 
    
    

// select * from spvs_by_user_by_week where recent_sign_up_flag  = 1 //checks



select
reporting_week_start_date

, sum(case when r.recent_sign_up_flag = 1 then r.total_margin end) as margin_from_recent_sign_ups
, sum(r.total_margin) as total_margin
, div0(sum(case when r.recent_sign_up_flag = 1 then r.total_margin end), sum(r.total_margin) ) as pct_margin_recent_signups
 
, sum(case when s.recent_sign_up_flag = 1 then s.spvs end) as spvs_from_recent_sign_ups
, sum(s.spvs) as total_spvs
, div0(sum(case when s.recent_sign_up_flag = 1 then spvs end), sum(spvs) ) as pct_spvs_recent_signups
 
 
from cal c
left join revenue_by_week_by_user r on date(c.reporting_week_start_date) =  date(r.reporting_week)
left join spvs_by_user_by_week s on date(c.reporting_week_start_date) = date(s.reporting_week)

group by 1
order by 1 desc
//select count(*) , count(distinct shiro_user_id) from sign_up     //1 row per cust

//, agg_per_user as (
//  select 
//  s.shiro_user_id
//  , sign_up_month
//  , spvs
//  , total_margin
//  from sign_up s
//  left join revenue_lw r on s.shiro_user_id = r.shiro_user_id
//  left join spvs_lw sp  on s.shiro_user_id = sp.shiro_user_id
//  )
//  
//select
//sign_up_month
//
//, sum(total_margin)
//, sum(spvs)
//, div0(sum(total_margin), total_total_margin) pct_of_total_margin
//, div0(sum(spvs), total_total_spvs) pct_of_total_spvs
//
//, total_total_margin
//, total_total_spvs
//
//from agg_per_user
//    cross join 
//    (select sum(total_margin) as total_total_margin
//          , sum(spvs) as total_total_spvs
//             from agg_per_user )
//group by 1, 6,7
//