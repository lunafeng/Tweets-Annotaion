# Tweets-Annotaion

Annotate tweets by using dominant senses found in DominantSenseDetection project

There are two ways to do annotation

i. Compute the similarity between each Dominant Sense with the context of the tweet, the sense with the highest score will be selected as the correct sense as the annotaion.

Usage
python annotation_noCD.py


ii. Compute the collective similarity between all the combination of senses for ambiguous words in a tweet. 

Usage
python annotaion_CD.py
