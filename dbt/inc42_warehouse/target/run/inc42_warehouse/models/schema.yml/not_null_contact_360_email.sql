
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select email
from `bigquery-296406`.`silver_gold`.`contact_360`
where email is null



  
  
      
    ) dbt_internal_test