create or replace external table  `project-75b24ad0-af2b-4b2b-863.dayasagar.photo_ext`
(
  caption	                STRING  
  ,gallery_intro            STRING  
  ,gallery_published_time	STRING  
  ,imageId                  STRING  
 
)

OPTIONS ( 
    format = 'CSV',
    field_delimiter = ',',
    skip_leading_rows = 1,
    ignore_unknown_values = TRUE,
    allow_jagged_rows = TRUE,
    uris = ['gs://photo-photos/*'] 
    
    );