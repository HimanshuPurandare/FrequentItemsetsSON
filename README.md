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
I. The SON algorithm is a 2-Phase MapReduce Algorithm.__
II. In Case 1, as an input to Phase 1 Map, we gave Male_UserIds mapped with the distinct MovieIds that they have rated i.e baskets (UserIds)
and Items (MovieId). (MaleUserId -> MovieIds)__
III. In Case 2, we pass MovieId mapped to distinct Female_UserIds who have rated it. These will be our buckets (MovieIds) and Items (UserIds)
for the apriori algorithm.__

### MAP 1:
IV. Then the first map task, we call Apriori algorithm and pass it chunks of baskets (say 4). We also pass it the relevant support “ps” where p = 1/no. of chunks.__
V. In the apriori algorithm, we first keep the count of all the distinct Items in the given chunk of baskets. Then, using the provided threshold (ps), we calculate the frequent items. These will be our singleton frequent Items.__
VI. Then later, we loop till the frequents generated are not empty. We then combine the frequents obtained from the previous step and remove
the duplicates.__
VII. Then the most important step is to remove the generated tuples, subsets of which don’t appear as the frequent pairs in the previous pass.__
VIII. Then again we count the generated candidates and then generate frequent tuples.__

### REDUCE 1:
IX. In here, we combine the output from all the map tasks and emit the distinct frequent tuples mapped to 1.__
X. In this phase, we eliminate False Negatives from the output. This uses the mootonicity property.__

### MAP 2:
XI. In this task, we give the entire output of Reduce 1 and a chunk to one map task.__
XII. Then we count the actual number of tuples.__
XIII. We emit Frequent Itemsets and their respective counts.__

### REDUCE 2:
XIV. In this task, we combine the counts of all the same frequent itemsets.__
XV. We will then get their actual count in entire dataset. Using this, we will then compare their counts with the actual support and eliminate the infrequent ones. This is the elimination of false positives.__
*Note:__ 
After getting the required output from MapReduce Tasks, I just formatted (sorted) the output to wite it to a file.
