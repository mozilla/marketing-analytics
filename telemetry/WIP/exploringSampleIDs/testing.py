aDAUUSBuckets2 = aDAUUSReturnUsage.groupBy('country_limited').agg(F.sum(F.when(aDAUUSReturnUsage['monthlyCount'] == 1, 1).otherwise(0)).alias('1'),
                                                                  F.sum(F.when((aDAUUSReturnUsage['monthlyCount'] > 1) & (aDAUUSReturnUsage['monthlyCount'] <= 5), 1).otherwise(0)).alias('2-5'),
                                                                  F.sum(F.when((aDAUUSReturnUsage['monthlyCount'] > 5) & (aDAUUSReturnUsage['monthlyCount'] <= 10), 1).otherwise(0)).alias('6-10'))
