
select  i_item_id,
        ca_country,
        ca_state, 
        ca_county,
        avg( cast(cs_quantity as double)) agg1,
        avg( cast(cs_list_price as double)) agg2,
        avg( cast(cs_coupon_amt as double)) agg3,
        avg( cast(cs_sales_price as double)) agg4,
        avg( cast(cs_net_profit as double)) agg5,
        avg( cast(c_birth_year as double)) agg6,
        avg( cast(cd1.cd_dep_count as double)) agg7
 from catalog_sales, customer_demographics cd1, 
      customer_demographics cd2, customer, customer_address, date_dim, item
 where cs_sold_date_sk = d_date_sk and
       cs_item_sk = i_item_sk and
       cs_bill_cdemo_sk = cd1.cd_demo_sk and
       cs_bill_customer_sk = c_customer_sk and
       cd1.cd_gender = 'M' and 
       cd1.cd_education_status = 'College' and
       c_current_cdemo_sk = cd2.cd_demo_sk and
       c_current_addr_sk = ca_address_sk and
       c_birth_month in (9,5,12,4,1,10) and
       d_year = 2001 and
       ca_state in ('ND','WI','AL'
                   ,'NC','OK','MS','TN')
 group by rollup (i_item_id, ca_country, ca_state, ca_county)
 order by ca_country desc,
        ca_state desc, 
        ca_county desc,
	i_item_id
  LIMIT 100 ;


