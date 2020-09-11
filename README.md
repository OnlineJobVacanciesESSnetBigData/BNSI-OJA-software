# BNSI-OJA-software
Bulgarian National Statistical Institute software used for Online Job Advertisements experimental statistics
## General description
This software is running on a Windows Server with Pyhton 3.7.2. It is scheduled to run every day. It scrapes data from web sites described in JSON files in format by webscraper.io Google Chrome extension. It is used for collecting data about OJA, accommodations and online prices in e-shops. The software process the scraped data every day and generates weekly, monthly and quarterly reports with experimental statistical data.
Currenty, BNSI is scraping jobs.bg and zaplata.bg web sites for OJA.
## specific_spyder.py
This is a Python Scrapy spyder. It uses JSON files with description what to scrap from which web site. 
The script is called with command like this:
scrapy crawl specific -a tag=jobs.bg
