# Scala and spark project to determine frequent Itemsets using SON algorithm.

## Commands to Run:
spark-submit --class "Himanshu_Purandare_SON" --master local[*] Himanshu_Purandare_SON.jar <Case_To_Run> "<Path_To_User_File>"
"<Path_To_Ratings_File>" <Support>

Example:
spark-submit --class "Himanshu_Purandare_SON" --master local[*] Himanshu_Purandare_SON.jar 2 "/home/test/SampleApp/users.dat"
"/home/test/SampleApp/ratings.dat" 600

* Note: 
The arguments to be passed should be in the given specified order.

## Description:
#### Task 1:
Calculate the combinations of frequent movies (as singletons, pairs, triples, etc...) that were rated by male users and are qualified as frequent given a support threshold value.

In order to apply this computation, we will need to create a basket for each male user containing the ids of the movies that were rated by this male user. If a movie was rated more than one time from a user, we consider that this movie was rated only once. More specifically, the movie ids are unique within each basket. The generated baskets are similar to:

* Male-user1 = (movie 11 , movie 12 , movie 13 , ...)
* Male-user2 = (movie 21 , movie 22 , movie 23 , ...)
* Male-user3 = (movie 31 , movie 32 , movie 33 , ...)

#### Task 2:
Calculate the combinations of frequent female users (as singletons, pairs,triples, etc...) who rated the movies. The frequent combination of female users has to be calculated on the basis of the support threshold value.
In order to apply this computation, we will need to create a basket for each movie that has been rated by the female users. More specifically, the female users ids are unique within each basket. The generated baskets are similar to:

* Movie 1 = (female-user 1 , female-user 2 , female-user 3 , ...)
* Movie 2 = (female-user 3 , female-user 4 , female-user 5 , ...)
* Movie 3 = (female-user 1 , female-user 2 , female-user 5 , ...)

I. The SON algorithm is a 2-Phase MapReduce Algorithm.<br />
II. In Case 1, as an input to Phase 1 Map, we gave Male_UserIds mapped with the distinct MovieIds that they have rated i.e baskets (UserIds)
and Items (MovieId). (MaleUserId -> MovieIds)<br />
III. In Case 2, we pass MovieId mapped to distinct Female_UserIds who have rated it. These will be our buckets (MovieIds) and Items (UserIds)
for the apriori algorithm.<br />

### MAP 1:
IV. Then the first map task, we call Apriori algorithm and pass it chunks of baskets (say 4). We also pass it the relevant support “ps” where p = 1/no. of chunks.<br />
V. In the apriori algorithm, we first keep the count of all the distinct Items in the given chunk of baskets. Then, using the provided threshold (ps), we calculate the frequent items. These will be our singleton frequent Items.<br />
VI. Then later, we loop till the frequents generated are not empty. We then combine the frequents obtained from the previous step and remove
the duplicates.<br />
VII. Then the most important step is to remove the generated tuples, subsets of which don’t appear as the frequent pairs in the previous pass.<br />
VIII. Then again we count the generated candidates and then generate frequent tuples.<br />

### REDUCE 1:
IX. In here, we combine the output from all the map tasks and emit the distinct frequent tuples mapped to 1.<br />
X. In this phase, we eliminate False Negatives from the output. This uses the mootonicity property.<br />

### MAP 2:
XI. In this task, we give the entire output of Reduce 1 and a chunk to one map task.<br />
XII. Then we count the actual number of tuples.<br />
XIII. We emit Frequent Itemsets and their respective counts.<br />

### REDUCE 2:
XIV. In this task, we combine the counts of all the same frequent itemsets.<br />
XV. We will then get their actual count in entire dataset. Using this, we will then compare their counts with the actual support and eliminate the infrequent ones. This is the elimination of false positives.<br />
* Note:<br />
After getting the required output from MapReduce Tasks, I just formatted (sorted) the output to wite it to a file.
