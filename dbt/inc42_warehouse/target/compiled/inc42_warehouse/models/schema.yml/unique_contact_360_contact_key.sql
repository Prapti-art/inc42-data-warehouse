
    
    

with dbt_test__target as (

  select contact_key as unique_field
  from `bigquery-296406`.`silver_gold`.`contact_360`
  where contact_key is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


