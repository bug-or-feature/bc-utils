# Release notes

## 0.1.7 (2024-12-28)
* updated date format in hourly prices (#27)
* file updates include Volume instead of Open Interest (#25)
* allow update of split frequency price files
* fixed config for US30
* new markets in config

## 0.1.6 (2024-06-13)
* additional markets
* updated URL

## 0.1.5 (2024-02-19)
* restructured config, exchange specifies tick date and eod date
 
## 0.1.4 (2024-02-14)
* now downloads hourly and daily prices to separate files, eg Day_GOLD_20230200.csv, Hour_GOLD_20230400.csv
* better logic for handling periods with no or low data
* new script to update previously downloaded files
* new script to separate files from previous versions into split frequency files
* example code
* tests against Python 3.10
* black
 
## 0.1.3 (2023-01-24)
* newer and renamed instruments in config
* publish with token
* latest github action versions
* fixing lint warnings
* fixing deepsource warnings
