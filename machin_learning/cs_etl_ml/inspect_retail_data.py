
import pandas as pd

# 1. Read the original dataset
df = pd.read_csv("data/online_retail_II.csv")

# 2. Start generating IDs after 18287
next_customer_id = 18288

# 3. Group rows by Invoice
for invoice, group in df.groupby("Invoice", sort=False):
    # Check whether this invoice group has missing Customer IDs
    if group["Customer ID"].isna().all():
        # Assign one new ID to every row in this invoice group
        df.loc[group.index, "Customer ID"] = next_customer_id

        # Move to the next new ID for the next missing invoice group
        next_customer_id += 1

# 4. Save the updated data separately
df.to_csv("data/online_retail_II_customer_ids_filled.csv", index=False)

# 5. Show results for Invoice and Customer ID only
print("Missing Customer IDs remaining:", df["Customer ID"].isna().sum())
print("Next available generated ID:", next_customer_id)

print("\nExample invoice groups:")
print(
    df.loc[df["Invoice"].astype(str) == "489548", ["Invoice", "Customer ID"]]
    .drop_duplicates()
)



# Verify that each invoice has only one Customer ID
customer_ids_per_invoice = df.groupby("Invoice")["Customer ID"].nunique()

print(
    "Invoice groups with multiple Customer IDs:",
    (customer_ids_per_invoice > 1).sum()
)

print(
    "Invoice groups with missing Customer IDs:",
    df.groupby("Invoice")["Customer ID"].apply(
        lambda ids: ids.isna().any()
    ).sum()
)
