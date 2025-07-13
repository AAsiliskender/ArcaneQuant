################# DEVNOTES #################


##### STRATEGIC TO DO LIST:
- Install linting/create lint reporting system (for transparency)
- Create DataManager class to manage DownloadIntraday, ExtractData, and Contextualise (to create)


#### SHORT-TERM TO DO:
- Create Contextualise (use meta-data to fix stuff like date-time)
- Update the separate files (in arcanequant) with code in jupy (as it is updated/cleaned/fixed)
- Move ExtractData etc. into a file and remove from jupy
- Enable jupy to use these files when running other stuff (also increases work speed)

## OPTIONAL TO DO:
- Putting (SQL query) test inputs and expected outputs into json/yaml for cleanliness of test files 


# NOTES:
- Currently when getting data from SQL, the inferred data (nominal price) is ignored, maybe can fix it?
- ExtractData might be inefficient when filtering for months (esp when using 'all'), check this out