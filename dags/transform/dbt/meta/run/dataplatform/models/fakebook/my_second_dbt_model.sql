
  
    

    create or replace table `data-platform-457606`.`dbt_bbagins`.`my_second_dbt_model`
      
    
    

    OPTIONS(
      description="""A starter dbt model"""
    )
    as (
      -- Use the `ref` function to select from other models

select *
from `data-platform-457606`.`dbt_bbagins`.`my_first_dbt_model`
where id = 1
    );
  