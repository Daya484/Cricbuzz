create or replace external table  `project-75b24ad0-af2b-4b2b-863.dayasagar.icc_ranking_allrounder_ext`
(

rank	          STRING,
id	            STRING,
name	          STRING,
country	        STRING,
countryId	      STRING,
rating	        STRING,
points	        STRING,
difference	    STRING,
trend	          STRING,
lastUpdatedOn	  STRING,
faceImageId     STRING 

)

OPTIONS ( 
    format = 'CSV',
    field_delimiter = ',',
    skip_leading_rows = 1,
    ignore_unknown_values = TRUE,
    allow_jagged_rows = TRUE,
    uris = ['gs://icc-rank_allrounder/*'] 
    
    );