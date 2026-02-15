create or replace external table  `project-75b24ad0-af2b-4b2b-863.dayasagar.dim_market`
(
  alpha2	          STRING  
  ,alpha3	          STRING  
  ,country	        STRING  
  ,region	          STRING  
  ,currency_code	  STRING  
  ,currency_name    STRING  
 
)

OPTIONS ( 
    format = 'CSV',
    field_delimiter = ',',
    skip_leading_rows = 1,
    ignore_unknown_values = TRUE,
    allow_jagged_rows = TRUE,
    uris = ['gs://market-details/*'] 
    
    );