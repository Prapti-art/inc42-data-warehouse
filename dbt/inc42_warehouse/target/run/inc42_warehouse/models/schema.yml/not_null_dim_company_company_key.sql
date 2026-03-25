
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select company_key
from `bigquery-296406`.`silver_gold`.`dim_company`
where company_key is null



  
  
      
    ) dbt_internal_test