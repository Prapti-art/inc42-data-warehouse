
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select unified_contact_id
from `bigquery-296406`.`silver_silver`.`contacts`
where unified_contact_id is null



  
  
      
    ) dbt_internal_test