
    
    

with dbt_test__target as (

  select company_id as unique_field
  from `bigquery-296406`.`silver_silver`.`companies`
  where company_id is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


