# Movie Data Wiki
Python program that manipulates movie data from wikidata.org and rottentomatoes.com to analyse the different genres, cast members and directors by different metrics.

# Getting Data
### Wikidata
Wikidata.org provides a JSON data dump of their whole database. This data has been disassembled by `split-wikidata.sh` and the data is transformed with `transform_download.py` with the command 
```
spark-submit data/transform_download.py {wikidata_location} 
```
`wikidata_location:` wherever the disassembled data is store. If on the SFU cluster HDFS, it can be found at `/courses/datasets/wikidata`
### Movie Data

`build_wikidata_movies.py` is then used to get the movie and mapping data.
```
spark-submit build_wikidata_movies.py {transformed_wikidata_location} {type}
```
`transformed_wikidata_location:` wherever the disassembled data is store. If on the SFU cluster HDFS, it can be found at `wikidata-useful-parquet`
`type:` Type of data. 
`build_wikidata_movies.py` needs to be run 4 different times to get the different types: `movies`, `genre`, `cast_member` and `director` 

The `json.gz` files should be stored in the `data/json` folder of the project

# Producing Results
### Comparison
Produces Tukey's Honest Significant Difference plot. Produces a `json.gz` of the cleaned data if one does not exists
```
python3 comparison.py -c {category} -s {score} -m {min_number_of_movies} -i {influencers} -y {start and end years}
```
`--category, -c:` Property to focus on (cast_member, director, genre)
`--score", -s:` Score to focus on (return, critic_percent, critic_average, audience_percent, audience_average)
`--movies", -m:` Minimum number of movies of each property (default=40)
`--influencers, -i:` Number of influential people/genres (default=25)
`--year, -y:` Bound the publication dates by the years (default=None)

Example:
```
python3 comparison.py -c genre -s critic_percent
python3 comparison py -c genre -s return -i 10 -y 1950 2000
```

Sample Input: `data\json\genre-critic_percent-40-10-1950-2000.json.gz`
Sample Output: `figures\genre-critic_percent-40-10-1950 2000.json.gz`

A two files will be produced. One in `data/json` of the cleaned wikidata and one in `figures` of a plot. The plot is a Tukey comparison of the top values of the category chosen by the score chosen. 

The name will match the parameters used (eg for the first command line, the file will be named  `genre-critic_percent-40-25`

### Yearly
Produces plot average score by the decade. Produces a `json.gz` of the cleaned data if one does not exists

```
python3 comparison.py -c {category} -s {score} -m {min_number_of_movies} -i {influencers} -y {start and end years}
```
`--category, -c:` Property to focus on (cast_member, director, genre)
`--score", -s:` Score to focus on (return, critic_percent, critic_average, audience_percent, audience_average)
`--movies", -m:` Minimum number of movies of each property (default=40)
`--influencers, -i:` Number of influential people/genres (default=25)

Example:
```
python3 yearly.py -c genre -s critic_percent
python3 yearly py -c genre -s return -i 10
```
A plot is produced in the  `figures` figures

The name will match the parameters used (eg for the first command line, the file will be named  `yearly-genre-critic_percent-40-25`

Sample Input: `data\json\genre-critic_percent-40-10.json.gz`
Sample Output: `figures\yearly-genre-critic_percent-40-10.json.gz`

# Dependencies
- pyspark
- pandas
- numpy
- sklearn
- scipy
- statsmodels
- seaborn

