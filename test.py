from DB_FETCH_ETL_TOKENOMICS import fetch_tokenomics_news
from DB_FETCH_ETL_AIRDROP import fetch_and_push_airdrop_events

#test = fetch_tokenomics_news()
test = fetch_and_push_airdrop_events()

test.to_csv("tokenomics_news.csv", index=False, encoding="utf-8")

print(test.info())