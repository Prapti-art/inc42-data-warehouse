
    
    

with dbt_test__target as (

  select unified_contact_id as unique_field
  from `bigquery-296406`.`silver_silver`.`contacts`
  where unified_contact_id is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


