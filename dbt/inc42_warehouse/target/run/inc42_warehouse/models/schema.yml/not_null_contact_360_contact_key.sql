
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select contact_key
from `bigquery-296406`.`silver_gold`.`contact_360`
where contact_key is null



  
  
      
    ) dbt_internal_test