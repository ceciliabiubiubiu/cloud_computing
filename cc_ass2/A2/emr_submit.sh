#!/bin/bash


spark-submit \
    --master local[4] \
    AverageRatingPerGenre.py \
    --input file:///home/hadoop/data/ \
    --output file:///home/hadoop/rating_out/
	 

    
