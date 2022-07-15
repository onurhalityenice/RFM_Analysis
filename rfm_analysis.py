###############################################################
# Part 1: Preparing the Data
###############################################################

#Importing libraries to use
import pandas as pd
import datetime as dt

#Adjusting the display settings
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.float_format', lambda x: '%.2f' % x)

#Reading the dataset
df_ = pd.read_csv("Miuul/WEEK_3/RFM/rfm_data_20k.csv")

#Creating a dataframe copy
df = df_.copy()
df.head(10)

#Examining the dataframe
df.master_id.nunique()
df.shape
df.head(10)
df.columns
df.shape
df.describe().T
df.isnull().sum()
df.info()

#Omnichannel customers shop both online and offline platforms.
#We create new variables for the total number of purchases and spending of each customer.
df["order_number_total"] = df["order_num_total_ever_online"] + df["order_num_total_ever_offline"]
df["customer_value_total"] = df["customer_value_total_ever_offline"] + df["customer_value_total_ever_online"]

#Examination of variable types.
#Converting the type of variables that express a date
date_columns = [col for col in df.columns if 'date' in col]
df[date_columns] = df[date_columns].apply(pd.to_datetime)
df.info()

#Examination of the distribution of the number of customers in the shopping channels, the total number of products purchased and the total expenditures.
df.groupby("order_channel").agg({"master_id":"count",
                                 "order_number_total":"sum",
                                 "customer_value_total":"sum"})

#Ranking the top 10 customers with the most revenue.
df.sort_values("customer_value_total", ascending=False)[["master_id","customer_value_total"]][:10]

#Ranking the top 10 customers with the most orders.
df.sort_values("order_number_total", ascending=False)[["master_id","order_number_total"]][:10]

#Functionalization
def data_prep(dataframe):
    dataframe["order_number_total"] = dataframe["order_num_total_ever_online"] + dataframe["order_num_total_ever_offline"]
    dataframe["customer_value_total"] = dataframe["customer_value_total_ever_offline"] + dataframe["customer_value_total_ever_online"]
    date_columns = [col for col in df.columns if 'date' in col]
    df[date_columns] = df[date_columns].apply(pd.to_datetime)
    return df

df = df_.copy()
df = data_prep(df)

###############################################################
# Part 2: Calculating RFM Metrics
###############################################################

#To determine 2 days after the last shopping date in the data set as the analysis date
df["last_order_date"].max() # 2021-05-30
analysis_date = dt.datetime(2021,6,1)

#Creating a new rfm dataframe with customer_id, recency, frequency and monetary values
rfm=pd.DataFrame({"CustomerId": df["master_id"],
                   "Recency": (analysis_date - df["last_order_date"]).astype('timedelta64[D]'),
                   "Frequency": df["order_number_total"],
                   "Monetary": df["customer_value_total"]})
rfm.head()

###############################################################
# Part 3: Calculating RF and RFM Scores
###############################################################

#Converting Recency, Frequency and Monetary metrics to scores between 1-5 with the help of qcut
#Saving these scores as recency_score, frequency_score and monetary_score
rfm["recency_score"] = pd.qcut(rfm['Recency'], 5, labels=[5, 4, 3, 2, 1])
rfm["frequency_score"] = pd.qcut(rfm['Frequency'].rank(method="first"), 5, labels=[1, 2, 3, 4, 5])
rfm["monetary_score"] = pd.qcut(rfm['Monetary'], 5, labels=[1, 2, 3, 4, 5])

rfm.head()

#Expressing recency_score and frequency_score as a single variable and saving it as RF_SCORE
rfm["RF_SCORE"] = (rfm['recency_score'].astype(str) + rfm['frequency_score'].astype(str))

rfm.head()

#Expressing recency_score and frequency_score and monetary_score as a single variable and saving it as RFM_SCORE
rfm["RFM_SCORE"] = (rfm['recency_score'].astype(str) + rfm['frequency_score'].astype(str) + rfm['monetary_score'].astype(str))

rfm.head()

###############################################################
# Part 4: Definition of RF Scores as Segments
###############################################################

#Segment definition and conversion of RF_SCORE to segments with the help of defined seg_map so that the generated RFM scores can be explained more clearly.
seg_map = {
    r'[1-2][1-2]': 'hibernating',
    r'[1-2][3-4]': 'at_Risk',
    r'[1-2]5': 'cant_loose',
    r'3[1-2]': 'about_to_sleep',
    r'33': 'need_attention',
    r'[3-4][4-5]': 'loyal_customers',
    r'41': 'promising',
    r'51': 'new_customers',
    r'[4-5][2-3]': 'potential_loyalists',
    r'5[4-5]': 'champions'
}

rfm['segment'] = rfm['RF_SCORE'].replace(seg_map, regex=True)

rfm.head()

###############################################################
# GÖREV 5: Analyzing
###############################################################

#Examination of recency, frequnecy and monetary averages of segments
rfm[["segment", "Recency", "Frequency", "Monetary"]].groupby("segment").agg({"Recency": ["count", "mean"],
                                                                             "Frequency": "mean",
                                                                             "Monetary": "mean"})

rfm.head()

#Finding customers in the relevant profile for 2 cases with the help of RFM analysis and saving their customer IDs to csv

#The company includes a new women's shoe brand.
#The product prices of the included brand are above the general customer preferences.
#For this reason, it is desired to contact the customers in the profile that will be interested in the promotion of the brand and product sales.
#These customers are planned to be loyal and female shoppers.
#Save the id numbers of the customers to the csv file as new_brand_target_customer_id.cvs.
target_segments_customer_ids = rfm[rfm["segment"].isin(["champions","loyal_customers"])]["customer_id"]
cust_ids = df[(df["master_id"].isin(target_segments_customer_ids)) &(df["interested_in_categories_12"].str.contains("KADIN"))]["master_id"]
cust_ids.to_csv("new_brand_target_customer_id.csv", index=False)
cust_ids.shape
target_segments_customer_ids.head()
rfm.head()

#Up to 40% discount is planned for Men's and Children's products.
#It is intended to specifically target customers who are good customers in the past who are interested in categories related to this discount, but who have not shopped for a long time and who have just arrived.
#Save the ids of the customers in the appropriate profile to the csv file as discount_target_customer_ids.csv.
target_segments_customer_ids = rfm[rfm["segment"].isin(["cant_loose","hibernating","new_customers"])]["customer_id"]
cust_ids = df[(df["master_id"].isin(target_segments_customer_ids)) & ((df["interested_in_categories_12"].str.contains("ERKEK"))|(df["interested_in_categories_12"].str.contains("COCUK")))]["master_id"]
cust_ids.to_csv("discount_target_customer_ids.csv", index=False)

### THE END ###