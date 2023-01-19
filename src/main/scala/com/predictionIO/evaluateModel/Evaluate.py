from pyspark.sql import *
from pyspark.sql.functions import *
import predictionio

engine_client = predictionio.EngineClient(url="http://localhost:8000")

spark = SparkSession.builder.getOrCreate()


sqlUserPurchase =  """
select user_id, collect_set(cast(content_id as string)) as content_id from waka.waka_pd_fact_wishlist
where  data_date_key > 20220631
group by user_id
order by user_id
"""

# Lay' tat' ca? userid co' trong thang' 7 va` train vs dl thang' 6
# Neu' k co' trong data thi` se~ recommend popular item cho ho.
# The score of 0 means popular items are being returned.
# Popularity type of recommendation system which works on the principle of popularity
# and or anything which is in trend. These systems check about the product or movie
# which are in trend or are most popular among the users and directly recommend those

dfAllUserid = spark.sql(sqlUserPurchase).select(col("user_id")).orderBy(col("user_id"))
listAllUserId = dfAllUserid.collect()
dictUserRecommend = {} # Dict chua' userId va` list item id cua? cac' item dc recommend cho user do'
for row in listAllUserId:
    numOfReturnRecom = 4
    returnQuery = engine_client.send_query({"user": str(row["user_id"]), "num": numOfReturnRecom})
    # {'itemScores': [{'item': '24671', 'score': 9.247717}, {'item': '369
    listItemScore = returnQuery["itemScores"]
    #[{'item': '24671', 'score': 9.247717}, {'item': '36986
    listItem = map(lambda x: x["item"], listItemScore)
    # [24671, 36986, ...]
    dictUserRecommend.update({str(row["user_id"]): list(listItem)})
    # {1: [24671, 36986,...], 2: [23245, ...], ...}

with open('userRecommendPredictionIO.txt', 'w+') as f:
    for key, value in dictUserRecommend.items():
        f.write('%s:%s\n' % (key, value))

# Lay' ds cac' userid va` itemid ma` ho. wishlist/purchase trong thang' 7
dfUserPurchase = spark.sql(sqlUserPurchase)
listUserPurchase = dfUserPurchase.collect()
# row[userid: 1, content_id: [24671, 36986,...]]
dictUserPurchase = {} # Dict chua' cac' userid va` list itemid ma` ho. wishlist/purchase trong thang' 7
for row in listUserPurchase:
    dictUserPurchase.update({str(row["user_id"]): row["content_id"]})
    # {1: [24671, 36986, ...], 2: [23245, ...], ...}

with open('userPurchasePredictionIO.txt', 'w+') as f:
    for key, value in dictUserPurchase.items():
        f.write('%s:%s\n' % (key, value))

# __Recommendations__	__Precision @k's__	        __AP@3__
# [0, 0, 1]	            [0, 0, 1/3]	            (1/3)(1/3) = 0.11
# [0, 1, 1]	            [0, 1/2, 2/3]	        (1/3)[(1/2) + (2/3)] = 0.38
# [1, 1, 1]	            [1/1, 2/2, 3/3]	        (1/3)[(1) + (2/2) + (3/3)] = 1
def averagePrecisionAtK(list1, list2):
    # Average Precision At 4
    averagePrecision = 0
    j = 1
    for i in range(4):
        if list1[i] in list2:
            averagePrecision += (1/4)*(j/(i+1))
            print(list1[i], '-', str(j), '-', str(i+1), '-', str((1/4)*(j/(i+1))))
            j += 1
    print("--------------")
    return averagePrecision

dictResult = {} # Dict tong? hop. kq de? debug
totalPrecision = 0 # Total Precision at 4 of all users
for userid in dictUserPurchase.keys():
    averagePrecision = averagePrecisionAtK(dictUserRecommend[userid], dictUserPurchase[userid])
    totalPrecision += averagePrecision
    dictResult.update({str(userid): {"Recommend": dictUserRecommend[userid],
    "Purchase": dictUserPurchase[userid], "precision": averagePrecision}})
    #{6913688:{'Recommend': ['37064', '1365', '1344', '36920'], 'Purchase':
    # ['37064', '1365', '1344', '36479'], 'precision': 0.75}

with open('averagePrecisionAtKPredictionIO.txt', 'w+') as f:
    # Sort lai. theo precision cho de~ debug
    for key, value in sorted(dictResult.items(), key=lambda x_y: x_y[1]["precision"],reverse=True):
        f.write('%s:%s\n' % (key, value))

# Mean Average Precision
MAP = totalPrecision / len(dictUserPurchase) * 100
with open('MAPPredictionIO.txt', 'w+') as f:
    f.write("Mean Average Precision: "+ str(MAP)+"%")

#1.6793872469416538% => Coi nhu o? muc trung binh` so vs cac' Doc tren mang.
# for
# "eventNames": [
#     "wishlist", "read", "rate"
# ],
# "blacklistEvents": ["wishlist", "read"]
