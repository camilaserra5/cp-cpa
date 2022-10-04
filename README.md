# cp-cpa
A script to calculate the CPA (Argentinian postal code) based on an address.
The script does a scrapping of https://codigo-postal.co/argentina/ and generates a csv database with all arg CPAS
Then, given an address, it fuzzy-matches the street with the database and finds the closest match. It then obtains the CPA.
