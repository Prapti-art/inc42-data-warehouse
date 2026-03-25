
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select company_id
from `bigquery-296406`.`silver_silver`.`companies`
where company_id is null



  
  
      
    ) dbt_internal_test