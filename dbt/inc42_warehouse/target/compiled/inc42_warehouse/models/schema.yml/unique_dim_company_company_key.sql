
    
    

with dbt_test__target as (

  select company_key as unique_field
  from `bigquery-296406`.`silver_gold`.`dim_company`
  where company_key is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


