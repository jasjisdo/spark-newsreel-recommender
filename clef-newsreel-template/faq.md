# FAQ: CLEF-NewsREEL 
#### Does a baseline recommender algorithm exist for the challenge?
Yes, the template for the NewsREEL challenge provides a baseline recommender algorithm. The recommender algorithm suggests articles most recently requested by the set of all users (`most recently requested algorithm`). The recommender does neither provide personalized recommendations nor computes the popularity or the life time of news articles. The recommender supports concurrent requests but does not apply synchronization strategies. This may result in dirty read operation or even exceptions. Nevertheless, the recommender reaches a reasonable performance. Since the algorithm is simple and efficient, we decided to publish this algorithm as baseline recommender.
The `most recently requested algorithm` recommender algorithms is used as baseline algorithm in task 1 as well as in task 2.

*Note:* For task 2 there is an additional baseline strategy. See next question.

#### The dataset provided for task 2 already seems to contain recommendations. Can I use these data?
The dataset provided for task 2 has been created based on a log file. It contains the recommendation that were embedded while showing a requested article. Since the goal of the challenge is to provide better recommendations that those that have been shown in 2014, we kept the information in the dataset. We see this strategy as an additional baseline strategy for task 2. In the online evaluation (task 1) the information is not available.

#### Is it allowed to adapt the baseline recommender algorithm?
Yes, it is, we even recommend this. The provided baseline recommender shows the complete recommendation process, starting from receiving the requests to parsing the relevant data to computing recommended itemIDs to sending back a response.

### What if my recommender always recommends the same article?
The evaluator checks whether a recommended article is requested by the user in the next minutes after presenting the recommendations. If an articleID has been correctly recommended, the articleID is removed once from the `window` of valid articles. Recommending an articles several times only gives points if the user also requests the articles several times. In general, it is not a good idea to recommend always the same article.

### What is the meaning of articleID `0`?
The article with ID `0` presents a special article that must not be recommended. Recommendations referring to articleID `0` are handled as invalid.

### How does the evaluator handle lists that do not have the correct number of items?
The evaluator used in task 2 applies a handling different from the strategy used in the online evaluation. Result list having less items than the requested number of recommendations are extended by adding invalid article recommendations (using articleID `0`). Result lists having more than the requested number of entries are cut to the requested length.

